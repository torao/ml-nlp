package at.hazm.ml.nlp.knowledge

import at.hazm.core.db.LocalDB
import at.hazm.core.db._
import at.hazm.ml.nlp.knowledge.Cyclopedia.Term
import at.hazm.ml.nlp.normalize

class Cyclopedia private[knowledge](db:LocalDB) {
  db.trx { con =>
    con.createTable(
      """cyclopedia(
        |  id integer not null primary key autoincrement,
        |  source_id integer not null references source(id),
        |  language varchar(12) not null,
        |  term text not null,
        |  qualifier text not null,
        |  meanings text not null,
        |  url text)""".stripMargin)
    con.exec("create unique index if not exists cyclopedia_idx00 on cyclopedia(term, qualifier, source_id, language)")
    con.exec("create index if not exists cyclopedia_idx01 on cyclopedia(term)")
  }

  /**
    * 百科事典から指定された単語を検索します。
    *
    * @param term 検索する単語
    * @return 一致した単語
    */
  def search(term:String):Seq[Term] = db.trx { con =>
    val _term = normalize(term)
    con.query("select id,source_id,language,qualifier,meanings,url from cyclopedia where term=?", _term) { rs =>
      Term(rs.getInt(1), rs.getInt(2), rs.getString(3), _term, rs.getString(4), rs.getString(5), Option(rs.getString(6)))
    }.toList
  }

  /**
    * 百科事典から指定された単語を検索します。
    *
    * @param term 検索する単語
    * @return 一致した単語
    */
  def search(term:String, qualifier:String):Seq[Term] = db.trx { con =>
    val _term = normalize(term)
    val _qualifier = normalize(qualifier)
    con.query("select id,source_id,language,qualifier,meanings,url from cyclopedia where term=? and qualifier=?", _term, _qualifier) { rs =>
      Term(rs.getInt(1), rs.getInt(2), rs.getString(3), _term, rs.getString(4), rs.getString(5), Option(rs.getString(6)))
    }.toList
  }

  def register(sourceId:Int, language:String)(f:((String, String, String, Option[String]) => Unit) => Unit):(Int, Int) = db.trx { con =>
    con.setAutoCommit(false)
    con.exec("delete from cyclopedia where source_id=? and language=?", sourceId, language)
    var insert = 0
    var update = 0
    f({ (term, qualifier, meanings, url) =>
      if(con.exec("update cyclopedia set meanings=?, url=? where term=? and qualifier=? and source_id=? and language=?",
        meanings, url.orNull, term, qualifier, sourceId, language) == 0) {
        con.exec("insert into cyclopedia(term,qualifier,source_id,language,meanings,url) values(?,?,?,?,?,?)",
          term, qualifier, sourceId, language, meanings, url.orNull)
        insert += 1
      } else update += 1
    })
    con.commit()
    (insert, update)
  }

}

object Cyclopedia {

  case class Term(id:Int, sourceId:Int, language:String, term:String, qualifier:String, meanings:String, url:Option[String]) {
    private[this] def join(t1:String, t2:String):String = t1 + (if(t2.nonEmpty) s":$t2" else "")

    def name:String = join(term, qualifier)
  }

}