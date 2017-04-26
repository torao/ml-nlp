package at.hazm.ml.nlp.knowledge

import at.hazm.ml.io.Database
import at.hazm.ml.io.Database._
import at.hazm.ml.nlp.knowledge.Cyclopedia.Term

class Cyclopedia private[knowledge](db:Database) {
  db.trx { con =>
    con.createTable(
      """cyclopedia(
        |  id integer not null primary key autoincrement,
        |  source_id integer not null references source(id),
        |  term text not null,
        |  qualifier text not null,
        |  meanings text not null,
        |  url text)""".stripMargin)
    con.exec("create unique index if not exists cyclopedia_idx00 on cyclopedia(term, qualifier, source_id)")
  }

  def search(term:String):Seq[Term] = db.trx{ con =>
    con.query("select id,source_id,qualifier,meanings,url from cyclopedia where term=?", term){ rs =>
      Term(rs.getInt(1), rs.getInt(2), term, rs.getString(3), rs.getString(4), Option(rs.getString(5)))
    }.toList
  }

  def register(sourceId:Int)(f:((String,String,String,Option[String])=>Unit)=>Unit):(Int,Int) = db.trx { con =>
    con.setAutoCommit(false)
    con.exec("delete from cyclopedia where source_id=?", sourceId)
    var insert = 0
    var update = 0
    f({ (term, qualifier, meanings, url) =>
      if(con.exec("update cyclopedia set meanings=?, url=? where term=? and qualifier=? and source_id=?",
        meanings, url.orNull, term, qualifier, sourceId) == 0){
        con.exec("insert into cyclopedia(term,qualifier,source_id,meanings,url) values(?,?,?,?,?)",
          term, qualifier, sourceId, meanings, url.orNull)
        insert += 1
      } else update += 1
    })
    con.commit()
    (insert, update)
  }

}
object Cyclopedia {
  case class Term(id:Int, sourceId:Int, term:String, qualifier:String, meanings:String, url:Option[String])
}