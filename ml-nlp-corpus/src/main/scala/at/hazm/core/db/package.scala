package at.hazm.core

import java.sql.{Connection, PreparedStatement, ResultSet}

import at.hazm.core.db.Database.makeHash
import at.hazm.core.io.using
import org.slf4j.LoggerFactory

package object db {
  private[db] val logger = LoggerFactory.getLogger("at.hazm.core.db.SQL")

  private[db] def log[T](sql:String, args:Seq[Any])(f: => T):T = {
    val t0 = System.currentTimeMillis()
    try {
      val result = f
      sqlLog(sql, args, System.currentTimeMillis() - t0, sqlValue(result))
      result
    } catch {
      case ex:Throwable =>
        sqlLog(sql, args, System.currentTimeMillis() - t0, ex.toString)
        throw ex
    }
  }

  private[db] def sqlLog(sql:String, args:Seq[Any], msec:Long, result:String):Unit = {
    val _args = if(args.isEmpty) "" else args.map(sqlValue).mkString(" [", ", ", "]")
    logger.info(f"$sql%s;${_args} ${if(msec >= 0) f"$msec%,d" else "***"}ms => $result")
  }

  private[db] def sqlValue(value:Any):String = value match {
    case null => "NULL"
    case arg:String => "'" + (if(arg.length > 50) arg.take(50) + "..." else arg).replaceAll("'", "''") + "'"
    case arg:java.sql.Date => f"${arg.getTime}%tF"
    case arg:java.sql.Time => f"${arg.getTime}%tT"
    case arg:java.util.Date => f"$arg%tFT$arg%tT.$arg%tL"
    case Some(arg) => sqlValue(arg)
    case None => "None"
    case arg => arg.toString
  }

  implicit class _Connection(con:Connection) {
    def headOption[T](sql:String, args:Any*)(converter:(ResultSet) => T):Option[T] = using(con.prepareStatement(sql)) { stmt =>
      log(sql, args) {
        args.zipWithIndex.foreach { case (arg, i) => stmt.setObject(i + 1, arg) }
        using(stmt.executeQuery()) { rs =>
          if(rs.next()) Some(converter(rs)) else None
        }
      }
    }

    def head[T](sql:String, args:Any*)(converter:(ResultSet) => T):T = headOption(sql, args:_*)(converter).get

    def query[T](sql:String, args:Any*)(converter:(ResultSet) => T):Iterator[T] = {
      val t0 = System.currentTimeMillis()
      try {
        val stmt = con.prepareStatement(sql)
        args.zipWithIndex.foreach { case (arg, i) => stmt.setObject(i + 1, arg) }
        val rs = stmt.executeQuery()
        new Cursor[T](converter, rs, stmt, () => sqlLog(sql, args, System.currentTimeMillis() - t0, ""))
      } catch {
        case ex:Throwable =>
          sqlLog(sql, args, System.currentTimeMillis() - t0, ex.toString)
          throw ex
      }
    }

    def foreach(sql:String, args:Any*)(callback:(ResultSet) => Unit):Unit = {
      log(sql, args) {
        val stmt = con.prepareStatement(sql)
        args.zipWithIndex.foreach { case (arg, i) => stmt.setObject(i + 1, arg) }
        val rs = stmt.executeQuery()
        while(rs.next()) {
          callback(rs)
        }
      }
    }

    def exec(sql:String, args:Any*):Int = using(con.prepareStatement(sql)) { stmt =>
      log(sql, args) {
        args.zipWithIndex.foreach { case (arg, i) => stmt.setObject(i + 1, arg) }
        stmt.executeUpdate()
      }
    }

    def createTable[T](sql:String):Boolean = exec(s"create table if not exists $sql") > 0
  }

  class Cursor[T] private[db](converter:(ResultSet) => T, rs:ResultSet, r:AutoCloseable*) extends Iterator[T] with AutoCloseable {
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

  trait _KeyType[T] {
    /** このクラスが表す型の SQL 名。 */
    val typeName:String

    /** 結果セットからこのクラスが定義する型の値を取得します。 */
    def get(rs:ResultSet, i:Int):T
  }

  implicit object _IntKeyType extends _KeyType[Int] {
    override val typeName:String = "integer"

    def get(rs:ResultSet, i:Int):Int = rs.getInt(i)
  }

  implicit object _LongKeyType extends _KeyType[Long] {
    override val typeName:String = "bigint"

    def get(rs:ResultSet, i:Int):Long = rs.getLong(i)
  }

  implicit object _StringKeyType extends _KeyType[String] {
    override val typeName:String = "text"

    def get(rs:ResultSet, i:Int):String = rs.getString(i)
  }

  trait _ValueType[T] {
    val typeName:String
    def set(stmt:PreparedStatement, i:Int, value:T):Unit
    def get(rs:ResultSet, i:Int):T
    def hash(value:T):Int
    def equals(value1:T, value2:T):Boolean
  }

  trait _ValueTypeForStringColumn[T] extends _ValueType[T] {
    val typeName:String = "text"
    def set(stmt:PreparedStatement, i:Int, value:T):Unit = stmt.setString(i, to(value))
    def get(rs:ResultSet, i:Int):T = from(rs.getString(i))
    def hash(value:T):Int = makeHash(to(value))
    def equals(value1:T, value2:T):Boolean = to(value1) == to(value2)
    def from(text:String):T

    def to(value:T):String
  }

  trait _ValueTypeForBinaryColumn[T] extends _ValueType[T] {
    val typeName:String = "blob"
    def set(stmt:PreparedStatement, i:Int, value:T):Unit = stmt.setBytes(i, to(value))
    def get(rs:ResultSet, i:Int):T = from(rs.getBytes(i))
    def hash(value:T):Int = makeHash(to(value))
    def equals(value1:T, value2:T):Boolean = to(value1) sameElements to(value2)
    def from(text:Array[Byte]):T

    def to(value:T):Array[Byte]
  }

  implicit object _StringValueType extends _ValueTypeForStringColumn[String] {
    override def from(text:String):String = text

    override def to(value:String):String = value
  }

  implicit object _IntSeqValueType extends _ValueTypeForStringColumn[Seq[Int]] {
    override def from(text:String):Seq[Int] = text.split(" ").filter(_.nonEmpty).map(_.toInt)

    override def to(value:Seq[Int]):String = value.mkString(" ")
  }

}
