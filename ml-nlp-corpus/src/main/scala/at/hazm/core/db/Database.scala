package at.hazm.core.db

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.sql.{Connection, ResultSet}

import at.hazm.core.db.Database._SerialKeyType
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
  class KVS[K, V](table:String, keyColumn:String = "key", valueColumn:String = "value")(implicit keyType:_KeyType[K], valueType:_ValueType[V]) extends IndexedStore[K, V](this, table, keyColumn, valueColumn, uniqueValue = false)(keyType, valueType) {
    def set(key:K, value:V):Unit = db.trx { con =>
      con.exec(s"INSERT INTO $table($keyColumn, hash, $valueColumn) VALUES(?, ?, ?) ON CONFLICT($keyColumn) DO UPDATE SET $valueColumn=EXCLUDED.$valueColumn, hash=EXCLUDED.hash", keyType.param(key), valueType.hash(value), valueType.toStore(value))
    }
  }

  /**
    * ユニークな値を保存します。
    *
    * @param table       テーブル名
    * @param keyColumn   キーカラム名
    * @param valueColumn 値カラム名
    * @param valueType   値カラムのタイプ
    * @tparam V 値の型
    */
  class Index[V](table:String, keyColumn:String = "key", valueColumn:String = "value")(implicit valueType:_ValueType[V]) extends IndexedStore[Int, V](this, table, keyColumn, valueColumn, uniqueValue = true)(_SerialKeyType, valueType) {

    /**
      * 指定された値をこのインデックスに追加します。同じ値がすでに存在する場合は何もせず既存のインデックスを返します。
      *
      * @param value インデックスに追加する値
      * @return 値のインデックス
      */
    def add(value:V):Int = db.trx { con =>
      con.setAutoCommit(false)
      con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED)
      con.exec(s"LOCK TABLE $table")
      getIds(con, value).headOption match {
        case Some(index) =>
          con.commit()
          index
        case None =>
          con.exec(s"INSERT INTO $table(hash, $valueColumn) VALUES(?, ?)", valueType.hash(value), valueType.toStore(value))
          con.commit()
          indexOf(value)
      }
    }

    /**
      * 指定された値のインデックスを返します。
      *
      * @param value インデックスを参照する値
      * @return 0 から始まる値のインデックス (存在しない場合は負の値)
      */
    def indexOf(value:V):Int = db.trx { con => getIds(con, value).headOption.getOrElse(-1) }
  }

}

object Database {
  private[Database] val logger = LoggerFactory.getLogger(classOf[Database])

  private[Database] object _SerialKeyType extends _KeyType[Int] {
    override val typeName:String = "SERIAL"

    def get(rs:ResultSet, i:Int):Int = rs.getInt(i) - 1

    override def param(key:Int):Int = key + 1
  }


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
