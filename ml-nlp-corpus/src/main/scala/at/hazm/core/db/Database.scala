package at.hazm.core.db

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.sql.{Connection, SQLException}

import at.hazm.core.io.using
import org.apache.tomcat.jdbc.pool.{DataSource, PoolProperties}
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

  def trx[T](f:(Connection) => T):T = using(newConnection) { con =>
    val result = f(con)
    if(!con.getAutoCommit) {
      con.commit()
    }
    result
  }

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
  class KVS[K, V](table:String, keyColumn:String = "key", valueColumn:String = "value")(implicit keyType:_KeyType[K], valueType:_ValueType[V]) extends IndexedStore[K, V](this, table, keyColumn, valueColumn)(keyType, valueType) {
    def set(key:K, value:V):Unit = insertOrUpdate(key, value)
  }

  class Index[V](table:String, keyColumn:String = "key", valueColumn:String = "value", unique:Boolean = false)(implicit valueType:_ValueType[V]) extends IndexedStore[Int, V](this, table, keyColumn, valueColumn)(_IntKeyType, valueType) {

    def register(value:V):Int = db.trx { con =>
      val hash = valueType.hash(value)

      def append():Int = synchronized {
        val vs = valueType.toStore(value)
        val key = con.headOption(s"SELECT MAX($keyColumn) FROM $table")(_.getInt(1) + 1).getOrElse(0)
        con.exec(s"INSERT INTO $table($keyColumn, hash, $valueColumn) VALUES(?, ?, ?)", key, hash, vs)
        key
      }

      def retryLoop():Int = try {
        if(unique) {
          con.query(s"SELECT $keyColumn, $valueColumn FROM $table WHERE hash=?", hash) { r =>
            val key = r.getInt(1)
            (key, valueType.get(key, r, 2))
          }.toList.find(r => valueType.equals(r._2, value)) match {
            case Some((existingKey, _)) => existingKey
            case None => append()
          }
        } else append()
      } catch {
        case ex:SQLException if ex.getSQLState == "23505" =>
          retryLoop()
      }

      retryLoop()
    }

    def indexOf(value:V):Int = getIds(value).headOption.getOrElse(-1)
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
