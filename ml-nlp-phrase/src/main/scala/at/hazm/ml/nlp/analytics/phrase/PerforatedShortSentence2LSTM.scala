package at.hazm.ml.nlp.analytics.phrase

import java.io.File

import at.hazm.core.db.LocalDB
import at.hazm.ml.nlp.Corpus
import at.hazm.ml.nlp.analytics.phrase.Optimizer._

object PerforatedShortSentence2LSTM {
  /**
    *
    * @param args コーパスファイル
    */
  def main(args:Array[String]):Unit = {
    val file = new File(args.head)
    val db = new LocalDB(file, readOnly = true)
    val corpus = new Corpus(db, "wikipedia")

    val open = corpus.vocabulary.instanceOf("(").map(_._1).toSet
    val close = corpus.vocabulary.instanceOf(")").map(_._1).toSet
    //    println(f"${corpus.paragraphs.realSize}%,d paragraphs")
    //    println(f"${corpus.vocabulary.size}%,d morphs")
    corpus.paragraphs.toCursor(5).foreach { case (docId, doc) =>
      System.out.println(doc.sentences.head.dropBetween(open, close).makePlainText(corpus))
      doc.sentences.head.dropBetween(open, close).breakdown().foreach { s =>
        System.out.println("  " + s.makePlainText(corpus))
      }
    }
    db.close()
  }
}
