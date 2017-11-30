package at.hazm.ml.nlp.analytics.phrase

import java.io.File

import at.hazm.core.db.LocalDB
import at.hazm.ml.nlp.Corpus
import at.hazm.ml.nlp.Paragraph.{Clause, Sentence}

import scala.annotation.tailrec
import scala.collection.mutable

object Main {
  /**
    *
    * @param args コーパスファイル
    */
  def main(args:Array[String]):Unit = {
    val file = new File(args.head)
    val db = new LocalDB(file, readOnly = true)
    val corpus = new Corpus(db, "wikipedia")
//    println(f"${corpus.paragraphs.realSize}%,d paragraphs")
//    println(f"${corpus.vocabulary.size}%,d morphs")
    corpus.paragraphs.toCursor(5).foreach { case (docId, doc) =>

      @tailrec
      def _join(clause:Clause, map:Map[Int, Clause], buf:mutable.Buffer[Clause] = mutable.Buffer()):Seq[Clause] = {
        buf.append(clause)
        if(clause.link >= 0) _join(map(clause.link), map, buf) else buf
      }

      val clauses = doc.sentences.flatMap(_.clauses)
      val map = clauses.groupBy(_.id).mapValues(_.head)
      (map.keySet -- clauses.map(_.link).toSet).toSeq.map(id => map(id)).map { head =>
        Sentence(-1, _join(head, map))
      }.foreach { sentence =>
        System.out.println(sentence.makePlainText(corpus))
      }
    }
    db.close()
  }
}
