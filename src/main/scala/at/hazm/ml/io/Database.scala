package at.hazm.ml.io

import java.io.File
import java.sql.{Connection, DriverManager, ResultSet, Statement}

class Database(file:File) {

  def trx[T](f:(Connection) => T):T = using(DriverManager.getConnection("jdbc:sqlite:$file"))(f)

}

object Database {

  implicit class _Connection(con:Connection) {
    def headOption[T](sql:String, args:Any*)(converter:(ResultSet) => T):Option[T] = using(con.prepareStatement(sql)) { stmt =>
      args.zipWithIndex.foreach { case (arg, i) => stmt.setObject(i + 1, arg) }
      using(stmt.executeQuery()) { rs =>
        if (rs.next()) Some(converter(rs)) else None
      }
    }

    def head[T](sql:String, args:Any*)(converter:(ResultSet) => T):T = headOption(sql, args:_*)(converter).get

    def query[T](sql:String, args:Any*)(converter:(ResultSet) => T):Iterator[T] = {
      val stmt = con.prepareStatement(sql)
      args.zipWithIndex.foreach { case (arg, i) => stmt.setObject(i + 1, arg) }
      val rs = stmt.executeQuery()
      new RSIterator[T](rs, stmt, converter)
    }

    def exec(sql:String, args:Any*):Int = using(con.prepareStatement(sql)) { stmt =>
      args.zipWithIndex.foreach { case (arg, i) => stmt.setObject(i + 1, arg) }
      stmt.executeUpdate()
    }

    def createTable[T](sql:String):Boolean = exec(s"create table if not exists $sql") > 0
  }

  private[Database] class RSIterator[T](rs:ResultSet, stmt:Statement, converter:(ResultSet) => T) extends Iterator[T] {
    private[this] var nextState:Option[Boolean] = None

    override def hasNext:Boolean = nextState match {
      case Some(s) => s
      case None =>
        val _nextState = rs.next()
        nextState = Some(_nextState)
        if (!_nextState) {
          rs.close()
          stmt.close()
        }
        _nextState
    }

    override def next():T = nextState match {
      case Some(true) =>
        val result = converter(rs)
        nextState = None
        result
      case Some(false) =>
        throw new IllegalStateException("no more result")
      case None =>
        hasNext
        next()
    }
  }

}
