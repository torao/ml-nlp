package at.hazm.ml.nlp.tools

import java.io.File
import java.util.concurrent.Executors
import java.util.concurrent.atomic.LongAdder

import at.hazm.core.db.LocalDB
import at.hazm.core.io.using
import at.hazm.core.util.Diagnostics.Progress
import at.hazm.ml.etl.{FileSource, SplitLine}
import at.hazm.ml.nlp.ja.ParallelCaboCha
import at.hazm.ml.nlp.{Corpus, Text}
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}
import scala.util.{Failure, Success}

/**
  * @deprecated ml-nlp-extract を使用してください。
  * @param file データベースファイル
  */
class Wikipedia2Corpus(file:File) {

  import Wikipedia2Corpus.logger

  private[this] val db = new LocalDB(file)
  private[this] val corpus = new Corpus(db)

  /**
    * 指定された Extract 済み Wikipedia データ (id title content の TSV 圧縮形式) からコーパスを作成します。
    * データベースの `docs` テーブルにデータが存在する場合は処理をスキップします。
    *
    * @param src Wikipedia データファイル
    */
  def makeCorpus(src:File):Unit = {
    val threads = 4
    val executor = Executors.newFixedThreadPool(threads)
    implicit val _context:ExecutionContextExecutor = ExecutionContext.fromExecutor(executor)

    using(new ParallelCaboCha(threads)) { cabocha =>
      val docCount = countLines(src, db)
      val current = new LongAdder()
      current.add(corpus.paragraphs.size)
      (new Progress(src.getName, corpus.paragraphs.size, docCount)) { prog =>
        (new FileSource(src, gzip = true) :> new SplitLine()).drop(1).map { line:String =>
          val Array(id, title, content) = line.split("\t")
          (id.toInt, title, content)
        }.filter { case (id, title, _) =>
          !title.endsWith("一覧") && !title.contains("曖昧さ回避") && !corpus.paragraphs.exists(id)
        }.foreach { case (id, title, content) =>
          prog.report(current.longValue(), f"$title (${content.length}%,d文字)")
          cabocha.parse(id, Text.normalize(content), corpus).onComplete {
            case Success(para) =>
              // corpus.paragraphs.set(id, para)    // cabocha.parse() 内で行っている
              current.increment()
            // prog.report(current.longValue(), title)
            case Failure(ex) =>
              logger.error(s"in document $id", ex)
          }
        }
      }
    }
    logger.info(f"ドキュメント数: ${corpus.paragraphs.realSize}%,d 文書")
    logger.info(f"形態素数: ${corpus.vocabulary.size}%,d 単語")

    // TODO 最短文や抽象化文の作成
    executor.shutdown()
    db.close()
  }

}

object Wikipedia2Corpus {
  private[Wikipedia2Corpus] val logger = LoggerFactory.getLogger(getClass.getName.dropRight(1))

  def main(args:Array[String]):Unit = {
    new Wikipedia2Corpus(new File(args(0))).makeCorpus(new File(args(1)))
  }
}
