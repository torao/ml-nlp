package at.hazm.ml.nlp

import java.io.File
import java.sql.Connection

import at.hazm.ml.io.Database
import at.hazm.ml.io.Database._

class Corpus(db:Database) {

  def this(file:File) = this(new Database(file))

  class Vocabulary(namespace:String = ""){
    private[this] val table = if(namespace.isEmpty) "vocabulary" else s"vocabulary_$namespace"
    private[this] var _size = -1

    db.trx { con =>
      // コーパステーブルの作成
      con.createTable(s"$table(idx integer not null primary key, term text not null unique)")
      // コーパスの整合性を確認
      this._size = con.head(s"select count(*) from $table")(_.getInt(1))
      if (con.head(s"select count(*) from $table where idx<0 or idx>=?", _size)(_.getInt(1)) > 0) {
        throw new IllegalStateException(s"terms idx conflict in $table (term removed?)")
      }
    }

    def size:Int = _size

    def bulk(f:((String) => Int) => Unit):Unit = db.trx { con =>
      f({ term =>
        _register(con, term)
      })
    }

    def register(term:String):Int = db.trx { con =>
      _register(con, term)
    }

    def register(terms:Seq[String]):Map[String,Int] = db.trx { con =>
      val dterms = terms.distinct
      val in = dterms.map(_.replaceAll("\'", "\'\'")).mkString("'", "','", "'")
      val m1 = con.query(s"select idx, term from $table where term in ($in)"){ rs =>
        (rs.getString(2), rs.getInt(1))
      }.toMap
      val m2 = dterms.collect{ case term if ! m1.contains(term) =>
        term -> register(term)
      }.toMap
      m1 ++ m2
    }

    private[this] def _register(con:Connection, term:String):Int = {
      con.headOption(s"select idx from $table where term=?", term)(_.getInt(1)) match {
        case Some(index) => index
        case None =>
          con.exec(s"insert into $table(idx, term) values(?, ?)", _size, term)
          _size += 1
          _size - 1
      }
    }

    def indexOf(term:String):Int = db.trx { con =>
      con.headOption(s"select idx from $table where term=?", term)(_.getInt(1)).getOrElse(-1)
    }

    def prefixed(prefix:String):List[(Int, String)] = db.trx { con =>
      con.query(s"select idx, term from $table where term like '%' || ?", prefix) { rs => (rs.getInt(1), rs.getString(2)) }.toList
    }
  }
}
