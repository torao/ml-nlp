package at.hazm.ml.nlp

import java.io.File
import java.sql.Connection

import at.hazm.ml.io.Database
import at.hazm.ml.io.Database._

class Corpus(file:File) {
  private[this] val db = new Database(file)
  private[this] var size = -1
  db.trx { con =>
    // コーパステーブルの作成
    con.createTable("corpus(index integer not null primary key, term text not null unique)")
    // コーパスの整合性を確認
    this.size = con.head("select count(*) from corpus")(_.getInt(1))
    if (con.head("select count(*) from corpus where index<0 or index>=?", size)(_.getInt(1)) > 0) {
      throw new IllegalStateException(s"terms index conflict (term removed?)")
    }
  }

  def bulk(f:((String) => Int) => Unit):Unit = db.trx { con =>
    f({ term =>
      _register(con, term)
    })
  }

  def register(term:String):Int = db.trx { con =>
    _register(con, term)
  }

  private[this] def _register(con:Connection, term:String):Int = {
    con.headOption("select index from corpus where term=?", term)(_.getInt(1)) match {
      case Some(index) => index
      case None =>
        con.exec("insert into corpus(index, term) values(?, ?)", size, term)
        size += 1
        size - 1
    }
  }

  def indexOf(term:String):Int = db.trx { con =>
    con.headOption("select index from corpus where term=?", term)(_.getInt(1)).getOrElse(-1)
  }

  def prefixed(prefix:String):List[(Int, String)] = db.trx { con =>
    con.query("select index, term from corpus where term like '%' || ?", prefix) { rs => (rs.getInt(1), rs.getString(2)) }.toList
  }
}
