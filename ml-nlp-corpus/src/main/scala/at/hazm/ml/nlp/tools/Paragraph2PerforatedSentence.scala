package at.hazm.ml.nlp.tools

import at.hazm.core.util.Diagnostics.Progress
import at.hazm.ml.nlp.Corpus
import org.slf4j.LoggerFactory

/**
  * コーパス中のパラグラフを最短文にして登録する。
  */
object Paragraph2PerforatedSentence {
  private[this] val logger = LoggerFactory.getLogger(getClass.getName.dropRight(1))

  def init(corpus:Corpus):Unit = if(corpus.perforatedSentences.size == 0 || true) {
    (new Progress("perforate sentence", 0, corpus.paragraphs.size)) { prog =>
      corpus.paragraphs.foreach { case (_, paragraph) =>
        corpus.perforatedSentences.register(paragraph)
        prog.report(paragraph.id.toString)
      }
    }
  }

}