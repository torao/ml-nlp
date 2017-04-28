package at.hazm.ml.nlp.knowledge

import java.io.File

import at.hazm.ml.io.Database
import at.hazm.ml.nlp.normalize

import scala.collection.mutable

class Knowledge(file:File) {
  private[this] val db = new Database(file)

  val source = new Source.DB(db)
  val cyclopedia = new Cyclopedia(db)
  val synonyms = new Synonym(db)

  def findSynonyms(term:String, cyclopediaOnly:Boolean = false):Set[String] = {
    val search = normalize(term)
    val names = mutable.HashSet[String]()
    cyclopedia.search(search).sortBy(_.qualifier).foreach { t =>
      names += t.name
      if(!cyclopediaOnly) names ++= synonyms.reverseSearch(t.term).map(_.name)
    }
    synonyms.search(search) match {
      case Seq() =>
        names ++= synonyms.reverseSearch(search).map(_.name)
      case syns =>
        syns.sortBy(_.qualifier).foreach { term =>
          names += term.alterName
          if(!cyclopediaOnly) names ++= synonyms.reverseSearch(term.alterTerm).map(_.name)
        }
    }
    names.toSet
  }

  def findCyclopedia(term:String):Seq[Cyclopedia.Term] = {
    val (search, qualifier) = split(normalize(term))

    def existsOrElse(s:Seq[Cyclopedia.Term])(f: => Seq[Cyclopedia.Term]):Seq[Cyclopedia.Term] = {
      if(s.nonEmpty) s else f
    }

    existsOrElse(cyclopedia.search(search, qualifier)) {
      existsOrElse(cyclopedia.search(search)) {
        findSynonyms(search, cyclopediaOnly = true).toSeq.flatMap { t =>
          cyclopedia.search(t)
        }
      }
    }
  }

  private[this] def split(term:String):(String, String) = term.split(":", 2) match {
    case Array(t, q) => (t.trim(), q.trim())
    case Array(t) => (t, "")
  }

}
