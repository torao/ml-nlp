/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements; and to You under the Apache License, Version 2.0.
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package at.hazm.ml.nlp.corpus

import at.hazm.core.db.{Database, _}

/**
  * 親→子となるようなツリー型の文書間の関連を保存/参照するためのクラスです。
  *
  * @param db        データベース
  * @param tableName テーブル名
  */
class Relations private[nlp](db:Database, tableName:String) {

  db.trx { con =>
    con.createTable(s"$tableName(id SERIAL NOT NULL PRIMARY KEY, parent INTEGER NOT NULL, child INTEGER NOT NULL)")
    con.createIndex(s"$tableName(parent)")
    con.createIndex(s"$tableName(child)")
    con.createIndex(s"$tableName(parent,child)", unique = true)
  }

  def register(parent:Int, children:Int*):Boolean = db.trx { con =>
    val param = children.flatMap(child => Seq(parent, child))
    val placeholder = children.map(_ => "(?,?)").mkString(",")
    con.exec(s"INSERT INTO $tableName(parent, child) VALUES$placeholder ON CONFLICT DO NOTHING", param:_*) > 0
  }

  def register(rels:Seq[(Int, Seq[Int])]):Int = db.trx { con =>
    val param = rels.flatMap { case (parent, children) => children.flatMap(child => Seq(parent, child)) }
    val placeholder = rels.flatMap { case (parent, children) => children.map(_ => "(?,?)") }.mkString(",")
    con.exec(s"INSERT INTO $tableName(parent, child) VALUES$placeholder ON CONFLICT DO NOTHING", param:_*)
  }

  def getChildren(parent:Int):Seq[Int] = db.trx { con =>
    con.query(s"SELECT child FROM $tableName WHERE parent=?", parent)(_.getInt(1)).toList
  }

  def maxParentId:Int = db.trx { con =>
    con.headOption(s"SELECT MAX(parent) FROM $tableName")(_.getInt(1)).getOrElse(-1)
  }

}
