package at.hazm.ml.nlp.knowledge

import at.hazm.core.db.LocalDB
import at.hazm.core.db._
import at.hazm.ml.nlp.knowledge.Synonym.Term
import at.hazm.ml.nlp.normalize

import scala.annotation.tailrec

class Synonym private[knowledge](db:LocalDB){
  db.trx { con =>
    con.createTable(
      """synonyms(
        |  id integer not null primary key autoincrement,
        |  source_id integer not null references source(id),
        |  language varchar(12) not null,
        |  term text not null,
        |  qualifier text not null,
        |  alter_term text not null,
        |  alter_qualifier text not null
        |)""".stripMargin)
    con.exec("create unique index if not exists synonyms_idx00 on synonyms(source_id, language, term, qualifier, alter_term, alter_qualifier)")
    con.exec("create index if not exists synonyms_idx01 on synonyms(term)")
    con.exec("create index if not exists synonyms_idx02 on synonyms(alter_term)")
  }

  /**
    * 指定された単語に対するシノニムを検索します。
    *
    * @param term 検索する単語
    * @return 一致した単語
    */
  def search(term:String):Seq[Term] = db.trx { con =>
    val _term = normalize(term)
    con.query("select id,source_id,language,qualifier,alter_term,alter_qualifier from synonyms where term=?", _term) { rs =>
      Term(rs.getInt(1), rs.getInt(2), rs.getString(3), _term, rs.getString(4), rs.getString(5), rs.getString(6))
    }.toList
  }

  def searchAll(term:String):Seq[Term] = {
    @tailrec
    def deepSearch(newbies:Seq[Term], terms:Seq[Term] = Seq.empty, limit:Int = 10):Seq[Term] = if(limit == 0){
      terms ++ newbies
    } else {
      val exists = terms.map(_.id).toSet
      val depth = newbies.flatMap{ term => search(term.alterTerm) }.filterNot{ t => exists.contains(t.id) }
      if(depth.isEmpty){
        terms ++ newbies
      } else {
        deepSearch(depth, terms ++ newbies, limit - 1)
      }
    }
    deepSearch(search(term))
  }

  def reverseSearch(altTerm:String):Seq[Term] = db.trx { con =>
    val _term = normalize(altTerm)
    con.query("select id,source_id,language,term,qualifier,alter_term,alter_qualifier from synonyms where alter_term=?", _term) { rs =>
      Term(rs.getInt(1), rs.getInt(2), rs.getString(3), rs.getString(4), rs.getString(5), _term, rs.getString(6))
    }.toList
  }

  def register(sourceId:Int, language:String)(f:((String,String,String,String) => Unit) => Unit):(Int, Int) = db.trx { con =>
    con.setAutoCommit(false)
    con.exec("delete from synonyms where source_id=? and language=?", sourceId, language)
    var insert = 0
    var ignore = 0
    f({ (term, qualifier, alterTerm, alterQualifier) =>
        if(con.exec("insert or ignore into synonyms(source_id,language,term,qualifier,alter_term,alter_qualifier) values(?,?,?,?,?,?)",
          sourceId, language, term, qualifier, alterTerm, alterQualifier) == 1) {
          insert += 1
        } else {
          ignore += 1
        }
    })
    con.commit()
    (insert, ignore)
  }

}

object Synonym {
  case class Term(id:Int, sourceId:Int, language:String, term:String, qualifier:String, alterTerm:String, alterQualifier:String){
    private[this] def join(t1:String, t2:String):String = t1 + (if(t2.nonEmpty) s":$t2" else "")
    def name:String = join(term, qualifier)
    def alterName:String = join(alterTerm, alterQualifier)
    override def toString:String = s"$name -> $alterName"
  }
}