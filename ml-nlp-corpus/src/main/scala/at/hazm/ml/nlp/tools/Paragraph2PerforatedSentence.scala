package at.hazm.ml.nlp.tools

import at.hazm.core.util.Diagnostics.Progress
import at.hazm.ml.nlp.Corpus
import org.slf4j.LoggerFactory

/**
  * コーパス中のパラグラフを最短文にして登録する。
  */
object Paragraph2PerforatedSentence {
  private[this] val logger = LoggerFactory.getLogger(getClass.getName.dropRight(1))

  def init(corpus:Corpus):Unit = {
    val resume = corpus.perforatedSentences.chads.maxDocId
    val docSize = corpus.perforatedSentences.chads.docSize

    (new Progress("perforate sentence", docSize, corpus.documents.size)) { prog =>
      corpus.documents.toCursor("key >= ?", resume).foreach { case (_, doc) =>
        corpus.perforatedSentences.register(doc)
        prog.report(doc.id.toString)
      }
    }

  }
}