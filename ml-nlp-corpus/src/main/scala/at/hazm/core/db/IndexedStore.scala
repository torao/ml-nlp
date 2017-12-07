/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements; and to You under the Apache License, Version 2.0.
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package at.hazm.core.db

import java.io.PrintWriter

import at.hazm.core.io.using

/**
  * このデータベース内の 1 テーブルを KVS として使用するためのクラスです。
  * KVS のキーはよく使われる型を暗黙的に利用することができます。
  * {{{
  *   import at.hazm.core.db.Database._
  *   val kvs = new db.KVS("kvs")
  * }}}
  *
  * @param table   テーブル名
  * @param keyType KVS のキーに対する型クラス
  * @tparam K KVS のキーの型
  */
abstract class IndexedStore[K, V](protected val db:Database, val table:String, keyColumn:String, valueColumn:String)(implicit keyType:_KeyType[K], valueType:_ValueType[V]) {

  // テーブルの作成
  db.trx { con =>
    con.createTable(s"$table($keyColumn ${keyType.typeName} NOT NULL PRIMARY KEY, hash INTEGER NOT NULL, $valueColumn ${valueType.typeName} NOT NULL)")
    con.createIndex(s"$table(hash)")
  }

  def apply(key:K):V = get(key).getOrElse {
    throw new IllegalArgumentException(s"値が存在しません: $key")
  }

  def get(key:K):Option[V] = db.trx {
    _.headOption(s"SELECT $valueColumn FROM $table WHERE $keyColumn=?", key)(rs => valueType.get(key, rs, 1))
  }

  def getAll(key:K*):Map[K, V] = db.trx {
    _.query(s"SELECT $keyColumn, $valueColumn FROM $table WHERE $keyColumn IN (${key.mkString(",")})") { rs =>
      val key = keyType.get(rs, 1)
      key -> valueType.get(key, rs, 2)
    }.toMap
  }

  def getIds(value:V):Seq[K] = db.trx { con =>
    val hash = valueType.hash(value)
    con.query(s"SELECT $keyColumn, $valueColumn FROM $table WHERE hash=?", hash) { rs =>
      val key = keyType.get(rs, 1)
      (key, valueType.get(key, rs, 2))
    }.filter(x => valueType.equals(x._2, value)).map(_._1).toList
  }

  def randomSelect(r: => Double):(K, V) = db.trx { con =>
    val count = con.head(s"SELECT COUNT(*) FROM $table")(_.getInt(1))
    val index = (count * r).toInt
    con.head(s"SELECT $keyColumn, $valueColumn FROM $table ORDER BY hash LIMIT ?, 1", index) { rs =>
      val key = keyType.get(rs, 1)
      (key, valueType.get(key, rs, 2))
    }
  }

  protected def insertOrUpdate(key:K, value:V):Unit = db.trx { con =>
    val hash = valueType.hash(value)
    val vs = valueType.toStore(value)
    con.exec(s"INSERT INTO $table($keyColumn, hash, $valueColumn) VALUES(?, ?, ?) ON CONFLICT($keyColumn) DO UPDATE SET hash=?, $valueColumn=?", key, hash, vs, hash, vs)
  }

  def deleteAll():Unit = db.trx { con =>
    con.exec(s"TRUNCATE TABLE $table")
  }

  def foreach(f:(K, V) => Unit):Unit = db.trx { con =>
    con.foreach(s"SELECT $keyColumn, $valueColumn FROM $table ORDER BY $keyColumn") { rs =>
      val key = keyType.get(rs, 1)
      f(key, valueType.get(key, rs, 2))
    }
  }

  def foreachAfter(skip:K)(f:(K, V) => Unit):Unit = db.trx { con =>
    con.foreach(s"SELECT $keyColumn, $valueColumn FROM $table WHERE $keyColumn > ? ORDER BY $keyColumn", skip) { rs =>
      val key = keyType.get(rs, 1)
      (key, valueType.get(key, rs, 2))
    }
  }

  def toCursor:Cursor[(K, V)] = {
    val con = db.newConnection
    val stmt = con.prepareStatement(s"SELECT $keyColumn, $valueColumn FROM $table ORDER BY $keyColumn")
    val rs = stmt.executeQuery()
    new Cursor({ rs =>
      val key = keyType.get(rs, 1)
      (key, valueType.get(key, rs, 2))
    }, rs, stmt, con)
  }

  def toCursor(limit:Int, offset:Int = 0):Cursor[(K, V)] = {
    val con = db.newConnection
    val stmt = con.prepareStatement(s"SELECT $keyColumn, $valueColumn FROM $table ORDER BY $keyColumn LIMIT ? offset ?")
    stmt.setInt(1, limit)
    stmt.setInt(2, offset)
    val rs = stmt.executeQuery()
    new Cursor({ rs =>
      val key = keyType.get(rs, 1)
      (key, valueType.get(key, rs, 2))
    }, rs, stmt, con)
  }

  def toCursor(condition:String, args:Any*):Cursor[(K, V)] = {
    val con = db.newConnection
    val stmt = con.prepareStatement(s"SELECT $keyColumn, $valueColumn FROM $table WHERE $condition ORDER BY $keyColumn")
    args.zipWithIndex.foreach { case (arg, i) => stmt.setObject(i + 1, arg) }
    val rs = stmt.executeQuery()
    new Cursor({ rs =>
      val key = keyType.get(rs, 1)
      (key, valueType.get(key, rs, 2))
    }, rs, stmt, con)
  }

  def size:Int = db.trx(_.headOption(s"SELECT COUNT(*) FROM $table")(_.getInt(1))).getOrElse(0)

  def exists(key:K):Boolean = db.trx(_.headOption(s"SELECT COUNT(*) FROM $table WHERE $keyColumn=?", key)(_.getInt(1) > 0)).getOrElse(false)

  def export(out:PrintWriter, delim:String = "\t"):Unit = using(toCursor) { cursor =>
    cursor.foreach { case (key, value) =>
      out.println(s"${keyType.export(key)}$delim${valueType.export(value)}")
    }
  }

}
