package at.hazm.core.db

import java.io.PrintWriter
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.sql.{Connection, SQLException}
import java.util.concurrent.atomic.AtomicInteger

import at.hazm.core.io.using
import org.apache.tomcat.jdbc.pool.{DataSource, PoolProperties}
import org.h2.api.ErrorCode
import org.slf4j.LoggerFactory

/**
  * JDBC 接続先のデータベースを現すクラスです。
  *
  * @param url      JDBC URL
  * @param username ユーザ名
  * @param password パスワード
  */
class Database(val url:String, val username:String, val password:String, driver:String, readOnly:Boolean = false) extends AutoCloseable {

  /**
    * データソース。
    */
  private[this] val ds = {
    val prop = new PoolProperties()
    prop.setUrl(url)
    prop.setUsername(username)
    prop.setPassword(password)
    prop.setDriverClassName(driver)
    prop.setJmxEnabled(true)
    prop.setTestWhileIdle(false)
    prop.setValidationInterval(3000)
    prop.setRemoveAbandoned(false) // NOTE: 単一スレッド処理の場合1コネクションを長時間使用するため
    prop.setLogAbandoned(true)
    prop.setRemoveAbandonedTimeout(60 * 1000)
    prop.setInitialSize(1)
    prop.setMinIdle(1)
    prop.setMaxActive(10)
    prop.setMaxIdle(10)
    prop.setDefaultReadOnly(readOnly)
    prop.setDefaultAutoCommit(true)
    new DataSource(prop)
  }

  /**
    * 新しいデータベース接続を作成します。
    *
    * @return
    */
  def newConnection:Connection = ds.getConnection

  /**
    * このデータベースが使用しているデータソースをクローズします。
    */
  override def close():Unit = ds.close()

  def trx[T](f:(Connection) => T):T = using(newConnection)(f)

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
  class KVS[K, V](table:String)(implicit keyType:_KeyType[K], valueType:_ValueType[V]) {
    trx { con =>
      con.createTable(s"$table(key ${keyType.typeName} NOT NULL PRIMARY KEY, hash INTEGER NOT NULL, value ${valueType.typeName} NOT NULL)")
      con.exec(s"CREATE INDEX IF NOT EXISTS ${table}_idx00 ON $table(hash)")
    }

    private[this] val cachedSize = new AtomicInteger(realSize)

    def apply(key:K):V = get(key).getOrElse {
      throw new IllegalArgumentException(s"値が存在しません: $key")
    }

    def get(key:K):Option[V] = trx {
      _.headOption(s"SELECT value FROM $table WHERE key=?", key)(rs => valueType.get(rs, 1))
    }

    def getAll(key:K*):Map[K, V] = trx {
      _.query(s"SELECT key, value FROM $table WHERE key IN (${key.mkString(",")})") { rs =>
        keyType.get(rs, 1) -> valueType.get(rs, 2)
      }.toMap
    }

    def getIds(value:V):Seq[K] = trx { con =>
      val hash = valueType.hash(value)
      con.query(s"SELECT key, value FROM $table WHERE hash=?", hash) { rs =>
        (keyType.get(rs, 1), valueType.get(rs, 2))
      }.filter(x => valueType.equals(x._2, value)).map(_._1).toList
    }

    def randomSelect(r: => Double):(K, V) = trx { con =>
      val count = con.head(s"SELECT COUNT(*) FROM $table")(_.getInt(1))
      val index = (count * r).toInt
      con.head(s"SELECT key, value FROM $table ORDER BY hash LIMIT ?, 1", index) { rs =>
        (keyType.get(rs, 1), valueType.get(rs, 2))
      }
    }

    def set(key:K, value:V):Unit = trx { con =>
      val hash = valueType.hash(value)
      val vs = valueType.toStore(value)
      con.exec(s"INSERT INTO $table(key, hash, value) VALUES(?, ?, ?) ON CONFLICT(key) DO UPDATE SET hash=?, value=?", key, hash, vs, hash, vs)
      cachedSize.incrementAndGet()
    }

    def foreach(f:(K, V) => Unit):Unit = trx { con =>
      con.foreach(s"SELECT key, value FROM $table ORDER BY key")(rs => f(keyType.get(rs, 1), valueType.get(rs, 2)))
    }

    def toCursor:Cursor[(K, V)] = {
      val con = newConnection
      val stmt = con.prepareStatement(s"SELECT key, value FROM $table ORDER BY key")
      val rs = stmt.executeQuery()
      new Cursor({ rs => (keyType.get(rs, 1), valueType.get(rs, 2)) }, rs, stmt, con)
    }

    def toCursor(limit:Int, offset:Int = 0):Cursor[(K, V)] = {
      val con = newConnection
      val stmt = con.prepareStatement(s"SELECT key, value FROM $table ORDER BY key LIMIT ? offset ?")
      stmt.setInt(1, limit)
      stmt.setInt(2, offset)
      val rs = stmt.executeQuery()
      new Cursor({ rs => (keyType.get(rs, 1), valueType.get(rs, 2)) }, rs, stmt, con)
    }

    def size:Int = cachedSize.get()

    def realSize:Int = trx(_.head(s"SELECT COUNT(*) FROM $table")(_.getInt(1)))

    def exists(key:K):Boolean = trx(_.head(s"SELECT COUNT(*) FROM $table WHERE key=?", key)(_.getInt(1)) > 0)

    def export(out:PrintWriter, delim:String = "\t"):Unit = using(toCursor) { cursor =>
      cursor.foreach { case (key, value) =>
        out.println(s"${keyType.export(key)}$delim${valueType.export(value)}")
      }
    }
  }

  class Index[V](table:String, unique:Boolean)(implicit valueType:_ValueType[V]) {
    private[this] val kvs = new KVS[Int, V](table)(_IntKeyType, valueType)

    def size:Int = kvs.size

    def get(id:Int):Option[V] = kvs.get(id)

    def register(value:V):Int = {
      def append(value:V):Int = {
        val id = kvs.size
        kvs.set(id, value)
        id
      }

      if(unique) {
        val id = indexOf(value)
        if(id >= 0) id else append(value)
      } else append(value)
    }

    def indexOf(value:V):Int = kvs.getIds(value).headOption.getOrElse(-1)
  }

}

object Database {
  private[Database] val logger = LoggerFactory.getLogger(classOf[Database])

  /**
    * 指定された文字列をハッシュ化して返します。この機能は長い文字列に対して検索用のインデックスを作成するために使用します。
    *
    * @param value ハッシュ化する文字列
    * @return 文字列のハッシュ値
    */
  def makeHash(value:String):Int = makeHash(value.take(50).getBytes(StandardCharsets.UTF_8))

  /**
    * 指定された文字列をハッシュ化して返します。この機能は長い文字列に対して検索用のインデックスを作成するために使用します。
    *
    * @param value ハッシュ化する文字列
    * @return 文字列のハッシュ値
    */
  def makeHash(value:Array[Byte]):Int = {
    val b = MessageDigest.getInstance("SHA-256").digest(value.take(50))
    (b(3) & 0xFF) << 24 | (b(2) & 0xFF) << 16 | (b(1) & 0xFF) << 8 | b(0) & 0xFF
  }

}
