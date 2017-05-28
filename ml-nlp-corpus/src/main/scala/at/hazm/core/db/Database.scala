package at.hazm.core.db

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.sql.{Connection, DriverManager}
import java.util.concurrent.atomic.AtomicInteger

import at.hazm.core.io.using
import org.slf4j.LoggerFactory

/**
  * JDBC 接続先のデータベースを現すクラスです。
  *
  * @param url      JDBC URL
  * @param username ユーザ名
  * @param password パスワード
  */
class Database(url:String, username:String, password:String) {

  /**
    * 新しいデータベース接続を作成します。
    *
    * @return
    */
  def newConnection:Connection = DriverManager.getConnection(url, username, password)

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
      con.createTable(s"$table(key ${keyType.typeName} not null primary key, hash ${valueType.typeName} not null, value text not null)")
      con.exec(s"create index if not exists ${table}_idx00 on $table(hash)")
    }

    private[this] val cachedSize = new AtomicInteger(realSize)

    def apply(key:K):V = get(key).getOrElse {
      throw new IllegalArgumentException(s"value not found: $key")
    }

    def get(key:K):Option[V] = trx {
      _.headOption(s"select value from $table where key=?", key)(rs => valueType.get(rs, 1))
    }

    def getAll(key:K*):Map[K, V] = trx {
      _.query(s"select key, value from $table where key in (${key.mkString(",")})") { rs =>
        keyType.get(rs, 1) -> valueType.get(rs, 2)
      }.toMap
    }

    def getIds(value:V):Seq[K] = trx { con =>
      val hash = valueType.hash(value)
      con.query(s"select key, value from $table where hash=?", hash) { rs =>
        (keyType.get(rs, 1), valueType.get(rs, 2))
      }.filter(x => valueType.equals(x._2, value)).map(_._1).toList
    }

    def randomSelect(r: => Double):(K, V) = trx { con =>
      val count = con.head(s"select count(*) from $table")(_.getInt(1))
      val index = (count * r).toInt
      con.head(s"select key, value from $table order by hash limit ?, 1", index) { rs =>
        (keyType.get(rs, 1), valueType.get(rs, 2))
      }
    }

    def set(key:K, value:V):Unit = trx { con =>
      val hash = valueType.hash(value)
      if(using(con.prepareStatement(s"update $table set value=?, hash=? where key=?")) { stmt =>
        valueType.set(stmt, 1, value)
        stmt.setInt(2, hash)
        stmt.setObject(3, key)
        stmt.executeUpdate()
      } == 0) {
        using(con.prepareStatement(s"insert into $table(key, hash, value) values(?, ?, ?)")) { stmt =>
          stmt.setObject(1, key)
          stmt.setInt(2, hash)
          valueType.set(stmt, 3, value)
          stmt.executeUpdate()
        }
        cachedSize.incrementAndGet()
      }
    }

    def foreach(f:(K, V) => Unit):Unit = trx { con =>
      con.foreach(s"select key, value from $table order by key") { rs =>
        f(keyType.get(rs, 1), valueType.get(rs, 2))
      }
    }

    def toCursor:Cursor[(K, V)] = {
      val con = newConnection
      val stmt = con.prepareStatement(s"select key, value from $table order by key")
      val rs = stmt.executeQuery()
      new Cursor({ rs => (keyType.get(rs, 1), valueType.get(rs, 2)) }, rs, stmt, con)
    }

    def size:Int = cachedSize.get()

    def realSize:Int = trx {
      _.head(s"select count(*) from $table")(_.getInt(1))
    }

    def exists(key:K):Boolean = trx {
      _.head(s"select count(*) from $table where key=?", key)(_.getInt(1)) > 0
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
    * 指定された文字列をハッシュ化して返します。この機能は長い文字列に対して検索用のインデックスを作成するために使用します。」
    *
    * @param value ハッシュ化する文字列
    * @return 文字列のハッシュ値
    */
  def makeHash(value:String):Int = makeHash(value.take(50).getBytes(StandardCharsets.UTF_8))

  /**
    * 指定された文字列をハッシュ化して返します。この機能は長い文字列に対して検索用のインデックスを作成するために使用します。」
    *
    * @param value ハッシュ化する文字列
    * @return 文字列のハッシュ値
    */
  def makeHash(value:Array[Byte]):Int = {
    val b = MessageDigest.getInstance("SHA-256").digest(value.take(50))
    (b(3) & 0xFF) << 24 | (b(2) & 0xFF) << 16 | (b(1) & 0xFF) << 8 | b(0) & 0xFF
  }

}
