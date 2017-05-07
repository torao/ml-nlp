package at.hazm.ml.io

import java.io.File
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.sql.{Connection, DriverManager, ResultSet}

class Database(file:File) {

  import Database._

  def newConnection:Connection = DriverManager.getConnection(s"jdbc:sqlite:$file")

  def trx[T](f:(Connection) => T):T = using(newConnection)(f)

  class KVS[T](table:String)(implicit impl:_KeyType[T]) {
    trx { con =>
      con.createTable(s"$table(key ${impl.dbType} not null primary key, hash integer not null, value text not null)")
      con.exec(s"create index if not exists ${table}_idx00 on $table(hash)")
    }

    def apply(key:T):String = get(key).getOrElse{
      throw new IllegalArgumentException(s"value not found: $key")
    }

    def get(key:T):Option[String] = trx {
      _.headOption(s"select value from $table where key=?", key)(_.getString(1))
    }

    def randomSelect(rng: =>Double):(T, String) = {
      val i = (size * rng).toInt
      trx {
        _.head(s"select key, value from $table limit ?, 1", i){ rs =>
          (impl.get(rs, 1), rs.getString(2))
        }
      }
    }

    def set(key:T, value:String):Unit = trx { con =>
      val hash = makeHash(value)
      if(con.exec(s"update $table set value=?, hash=? where key=?", value, hash, key) == 0) {
        con.exec(s"insert into $table(key, hash, value) values(?, ?, ?)", key, hash, value)
      }
    }

    def getIds(value:String):Seq[T] = trx { con =>
      val hash = makeHash(value)
      con.query(s"select key, value from $table where hash=?", hash){ rs =>
        (impl.get(rs, 1), rs.getString(2))
      }.filter(_._2 == value).map(_._1).toList
    }

    def foreach(f:(T, String) => Unit):Unit = trx { con =>
      con.foreach(s"select key, value from $table order by key") { rs =>
        f(impl.get(rs, 1), rs.getString(2))
      }
    }

    def toCursor:Cursor[(T,String)] = {
      val con = newConnection
      val stmt = con.prepareStatement(s"select key, value from $table order by key")
      val rs = stmt.executeQuery()
      new Cursor({ rs => (impl.get(rs, 1), rs.getString(2)) }, rs, stmt, con)
    }

    def size:Int = trx {
      _.head(s"select count(*) from $table")(_.getInt(1))
    }

    private[this] def makeHash(value:String):Int = {
      val b = MessageDigest.getInstance("SHA-256").digest(value.take(50).getBytes(StandardCharsets.UTF_8))
      (b(3) & 0xFF) << 24 | (b(2) & 0xFF) << 16 | (b(1) & 0xFF) << 8 | b(0) & 0xFF
    }

  }

}

object Database {
  trait _KeyType[T] {
    val dbType:String
    def get(rs:ResultSet, i:Int):T
  }
  implicit object _IntKeyType extends _KeyType[Int] {
    override val dbType:String = "integer"
    def get(rs:ResultSet, i:Int):Int = rs.getInt(i)
  }
  implicit object _LongKeyType extends _KeyType[Long] {
    override val dbType:String = "bigint"
    def get(rs:ResultSet, i:Int):Long = rs.getLong(i)
  }
  implicit object _StringKeyType extends _KeyType[String] {
    override val dbType:String = "text"
    def get(rs:ResultSet, i:Int):String = rs.getString(i)
  }

  implicit class _Connection(con:Connection) {
    def headOption[T](sql:String, args:Any*)(converter:(ResultSet) => T):Option[T] = using(con.prepareStatement(sql)) { stmt =>
      args.zipWithIndex.foreach { case (arg, i) => stmt.setObject(i + 1, arg) }
      using(stmt.executeQuery()) { rs =>
        if(rs.next()) Some(converter(rs)) else None
      }
    }

    def head[T](sql:String, args:Any*)(converter:(ResultSet) => T):T = headOption(sql, args:_*)(converter).get

    def query[T](sql:String, args:Any*)(converter:(ResultSet) => T):Iterator[T] = {
      val stmt = con.prepareStatement(sql)
      args.zipWithIndex.foreach { case (arg, i) => stmt.setObject(i + 1, arg) }
      val rs = stmt.executeQuery()
      new Cursor[T](converter, rs, stmt)
    }

    def foreach(sql:String, args:Any*)(callback:(ResultSet) => Unit):Unit = {
      val stmt = con.prepareStatement(sql)
      args.zipWithIndex.foreach { case (arg, i) => stmt.setObject(i + 1, arg) }
      val rs = stmt.executeQuery()
      while(rs.next()) {
        callback(rs)
      }
    }

    def exec(sql:String, args:Any*):Int = using(con.prepareStatement(sql)) { stmt =>
      args.zipWithIndex.foreach { case (arg, i) => stmt.setObject(i + 1, arg) }
      stmt.executeUpdate()
    }

    def createTable[T](sql:String):Boolean = exec(s"create table if not exists $sql") > 0
  }

  class Cursor[T] private[Database](converter:(ResultSet) => T, rs:ResultSet, r:AutoCloseable*) extends Iterator[T] with AutoCloseable {
    private[this] var nextState:Option[Boolean] = None
    private[this] var _count = 0L

    def count:Long = _count

    override def hasNext:Boolean = nextState match {
      case Some(s) => s
      case None =>
        val _nextState = rs.next()
        nextState = Some(_nextState)
        if(!_nextState) {
          close()
        }
        _nextState
    }

    override def next():T = nextState match {
      case Some(true) =>
        val result = converter(rs)
        nextState = None
        _count += 1
        result
      case Some(false) =>
        throw new IllegalStateException("no more result")
      case None =>
        hasNext
        next()
    }

    def close():Unit = {
      rs.close()
      r.foreach(_.close())
    }
  }

}
