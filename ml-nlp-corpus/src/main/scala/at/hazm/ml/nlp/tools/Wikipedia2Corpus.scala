package at.hazm.ml.nlp.tools

import java.io.File
import java.util.concurrent.Executors

import at.hazm.core.db.LocalDB
import at.hazm.core.io.{readText, using}
import at.hazm.core.util.Diagnostics
import at.hazm.core.util.Diagnostics.Progress
import at.hazm.ml.nlp.ja.ParallelCaboCha
import at.hazm.ml.nlp.{Corpus, Text}
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext

class Wikipedia2Corpus(file:File) {
  import Wikipedia2Corpus.logger

  private[this] val db = new LocalDB(file)
  private[this] val corpus = new Corpus(db, "wikipedia")

  /**
    * 指定された Extract 済み Wikipedia データ (id title content の TSV 圧縮形式) からコーパスを作成します。
    * データベースの `docs` テーブルにデータが存在する場合は処理をスキップします。
    *
    * @param src Wikipedia データファイル
    */
  def makeCorpus(src:File):Unit = {
    val threads = 4
    val docCount = countLines(src, db)
    val executor = Executors.newFixedThreadPool(threads)
    implicit val _context = ExecutionContext.fromExecutor(executor)
    val prog = new Progress(src.getName, corpus.paragraphs.size, docCount)
    using(new ParallelCaboCha(threads)) { cabocha =>
      readText(src) { in =>
        Iterator.continually(in.readLine()).takeWhile(_ != null).zipWithIndex.foreach { case (line, i) =>
          try {
            val Array(id, title, content) = line.split("\t")
            prog.report(i, f"$title (${content.length}%,d文字)")
            if(!title.endsWith("一覧") && !title.contains("曖昧さ回避") && !corpus.paragraphs.exists(id.toInt)) {
              Diagnostics.measureAsync("cabocha") {
                cabocha.parse(id.toInt, Text.normalize(content), corpus).andThen{ case _ =>
                  prog.report(i + 1, title)
                }
              }
            }
          } catch {
            case ex:MatchError =>
              throw new Exception(s"$src(${i + 1}): unexpected source file format: $line", ex)
          }
        }
      }
    }
    prog.stop()
    logger.info(f"ドキュメント数: ${corpus.paragraphs.realSize}%,d 文書")
    logger.info(f"形態素数: ${corpus.vocabulary.size}%,d 単語")

    // TODO 最短文や抽象化文の作成
  }

}

object Wikipedia2Corpus {
  private[Wikipedia2Corpus] val logger = LoggerFactory.getLogger(getClass.getName.dropRight(1))

  def main(args:Array[String]):Unit = {
    new Wikipedia2Corpus(new File(args(0))).makeCorpus(new File(args(1)))
  }
}
