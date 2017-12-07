package at.hazm.ml.nlp.tools

import at.hazm.core.util.Diagnostics.Progress
import at.hazm.core.util.ResourcePool
import at.hazm.ml.nlp.Corpus
import at.hazm.ml.nlp.model.{Morph, PerforatedDocument, RelativeDocument}
import org.slf4j.LoggerFactory

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContextExecutor, Future}

object Paragraph2PerforatedSentence {
  private[this] val logger = LoggerFactory.getLogger(getClass.getName.dropRight(1))

  def transform(corpus:Corpus, doc:RelativeDocument[Morph.Instance]):PerforatedDocument = {
    corpus.perforatedSentences.transform(doc)
  }

  /**
    * コーパス中のパラグラフを最短文にして登録する。
    *
    * @param corpus   コーパス
    * @param _context 並列実行のためのスレッドプール
    */
  def init(corpus:Corpus)(implicit _context:ExecutionContextExecutor):Unit = {
    logger.info(s"--- 最短文の生成とプレースホルダ化処理 ---")

    val resume = corpus.perforatedSentences.chads.maxDocId
    val docSize = corpus.perforatedSentences.documentSize
    logger.info(f"処理済み文書数: $docSize%,d 文書")
    logger.info(f"処理済み文書ID最大値: $resume")

    (new Progress("perforation", docSize, corpus.documents.size)) { prog =>
      val parallel = new ResourcePool(CPUs)("")
      val futures = corpus.documents.toCursor("key >= ?", resume).map { case (_, doc) =>
        parallel.acquireAndRun { _ =>
          corpus.perforatedSentences.register(doc)
        }.andThen { case _ =>
          prog.report(doc.id.toString)
        }
      }
      Await.ready(Future.sequence(futures), Duration.Inf)
    }

  }
}