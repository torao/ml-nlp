package at.hazm.core

import java.sql.{Connection, ResultSet}

import at.hazm.core.db.Database.makeHash
import at.hazm.core.io.using
import org.slf4j.LoggerFactory

package object db {
  private[db] val logger = LoggerFactory.getLogger("at.hazm.core.db.SQL")

  /**
    * Slow Query 警告対象の SQL 処理時間 (ミリ秒)
    */
  var millisecToWarnSlowQuery:Long = 10 * 1000L

  private[this] sealed abstract class LogLevel(_log:(String) => Unit, _check: => Boolean) {
    def isEnabled:Boolean = _check

    def log(msg:String):Unit = _log(msg)
  }

  private[this] case object ERROR extends LogLevel(logger.error, logger.isErrorEnabled)

  private[this] case object WARN extends LogLevel(logger.warn, logger.isWarnEnabled)

  private[this] case object INFO extends LogLevel(logger.info, logger.isInfoEnabled)

  private[db] def log[T](sql:String, args:Seq[Any])(f: => T):T = {
    val t0 = System.currentTimeMillis()
    try {
      val result = f
      val tm = System.currentTimeMillis() - t0
      if(tm < millisecToWarnSlowQuery) {
        sqlLog(sql, args, tm, sqlValue(result), INFO)
      } else {
        sqlLog(sql, args, tm, sqlValue(result) + " (slow query)", WARN)
      }
      result
    } catch {
      case ex:Throwable =>
        sqlLog(sql, args, System.currentTimeMillis() - t0, ex.toString, ERROR)
        throw ex
    }
  }

  private[db] def sqlLog(sql:String, args:Seq[Any], msec:Long, result:String, level:LogLevel):Unit = if(level.isEnabled) {
    val _args = if(args.isEmpty) "" else args.map(sqlValue).mkString(" [", ", ", "]")
    level.log(f"$sql%s;${_args} ${if(msec >= 0) f"$msec%,d" else "***"}ms => $result")
  }

  private[db] def sqlValue(value:Any):String = value match {
    case null => "NULL"
    case arg:String => "'" + (if(arg.length > 50) arg.take(50) + "..." else arg).replaceAll("'", "''") + "'"
    case arg:java.sql.Date => f"${arg.getTime}%tF"
    case arg:java.sql.Time => f"${arg.getTime}%tT"
    case arg:java.util.Date => f"$arg%tFT$arg%tT.$arg%tL"
    case Some(arg) => sqlValue(arg)
    case None => "None"
    case bin:Array[_] if bin.getClass.getComponentType == classOf[Byte] =>
      (if(bin.length <= 25) bin else bin.take(25)).collect { case b:Byte => f"$b%02X" }.mkString +
        (if(bin.length > 25) "..." else "") + f":${bin.length}%,d"
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
        new Cursor[T](converter, rs, stmt, { () =>
          val tm = System.currentTimeMillis() - t0
          if(tm < millisecToWarnSlowQuery) {
            sqlLog(sql, args, tm, "", INFO)
          } else {
            sqlLog(sql, args, tm, "(slow query)", WARN)
          }
        })
      } catch {
        case ex:Throwable =>
          sqlLog(sql, args, System.currentTimeMillis() - t0, ex.toString, ERROR)
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

    def exec(sql:String, args:Any*):Int = log(sql, args) {
      using(con.prepareStatement(sql)) { stmt =>
        args.zipWithIndex.foreach { case (arg, i) => stmt.setObject(i + 1, arg) }
        stmt.executeUpdate()
      }
    }

    def createTable(sql:String):Boolean = exec(s"CREATE TABLE IF NOT EXISTS $sql") > 0

    def createIndex(sql:String, unique:Boolean = false):Boolean = exec(s"CREATE ${if(unique) "UNIQUE " else ""}INDEX IF NOT EXISTS $sql") > 0
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

    /** データをエクスポート用に文字列変換 */
    def export(value:T):String = value.toString
  }

  implicit object _IntKeyType extends _KeyType[Int] {
    override val typeName:String = "INTEGER"

    def get(rs:ResultSet, i:Int):Int = rs.getInt(i)
  }

  implicit object _LongKeyType extends _KeyType[Long] {
    override val typeName:String = "BIGINT"

    def get(rs:ResultSet, i:Int):Long = rs.getLong(i)
  }

  implicit object _StringKeyType extends _KeyType[String] {
    override val typeName:String = "VARCHAR"

    def get(rs:ResultSet, i:Int):String = rs.getString(i)

    override def export(value:String):String = {
      val i = value.indexWhere(ch => ch == '\"' || Character.isISOControl(ch))
      if(i < 0) value else {
        "\"" + value.drop(i).foldLeft(new StringBuilder(value.substring(0, i))) { case (buffer, ch) =>
          if(ch == '\"') buffer.append("\"\"") else buffer.append(ch)
          buffer
        } + "\""
      }
    }
  }

  trait _ValueType[T] {
    val typeName:String

    def toStore(value:T):Object

    def get(rs:ResultSet, i:Int):T

    def hash(value:T):Int

    def equals(value1:T, value2:T):Boolean

    /** データをエクスポート用に文字列変換 */
    def export(value:T):String = value.toString
  }

  trait _ValueTypeForStringColumn[T] extends _ValueType[T] {
    val typeName:String = "TEXT"

    def toStore(value:T):Object = to(value)

    def get(rs:ResultSet, i:Int):T = from(rs.getString(i))

    def hash(value:T):Int = makeHash(to(value))

    def equals(value1:T, value2:T):Boolean = to(value1) == to(value2)

    def from(text:String):T

    def to(value:T):String
  }

  trait _ValueTypeForBinaryColumn[T] extends _ValueType[T] {
    val typeName:String = "BYTEA"

    def toStore(value:T):Object = to(value)

    def get(rs:ResultSet, i:Int):T = from(rs.getBytes(i))

    def hash(value:T):Int = makeHash(to(value))

    def equals(value1:T, value2:T):Boolean = to(value1) sameElements to(value2)

    def from(text:Array[Byte]):T

    def to(value:T):Array[Byte]
  }

  implicit object _StringValueType extends _ValueTypeForStringColumn[String] {
    override def from(text:String):String = text

    override def to(value:String):String = value

    override def export(value:String):String = _StringKeyType.export(value)
  }

  implicit object _IntSeqValueType extends _ValueTypeForStringColumn[Seq[Int]] {
    override def from(text:String):Seq[Int] = text.split(" ").filter(_.nonEmpty).map(_.toInt)

    override def to(value:Seq[Int]):String = value.mkString(" ")

    override def export(value:Seq[Int]):String = value.mkString(" ")
  }

  implicit object _DoubleSeqValueType extends _ValueTypeForStringColumn[Seq[Double]] {
    override def from(text:String):Seq[Double] = text.split(" ").filter(_.nonEmpty).map(_.toDouble)

    override def to(value:Seq[Double]):String = value.mkString(" ")

    override def export(value:Seq[Double]):String = value.mkString(" ")
  }

}
