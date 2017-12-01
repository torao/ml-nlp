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
      System.out.println(doc.sentences.head.makePlainText(corpus))
      doc.sentences.head.breakdown().foreach { s =>
        System.out.println("  " + s.makePlainText(corpus))
      }
    }
    db.close()
  }
}
