package at.hazm.ml.nlp.tools

import java.io.File
import java.util.concurrent.Executors
import java.util.concurrent.atomic.LongAdder

import at.hazm.core.db.Database
import at.hazm.core.io.using
import at.hazm.core.util.Diagnostics.Progress
import at.hazm.ml.etl.{FileSource, TextLine}
import at.hazm.ml.nlp.ja.{CaboCha, ParallelCaboCha}
import at.hazm.ml.nlp.model.{Morph, RelativeDocument}
import at.hazm.ml.nlp.{Corpus, Text}
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}
import scala.util.{Failure, Success}

/**
  * @deprecated ml-nlp-extract を使用してください。
  */
class Wikipedia2Corpus {

  import Wikipedia2Corpus.logger

  /**
    * 指定された Extract 済み Wikipedia データ (id title content の TSV 圧縮形式) からコーパスを作成します。
    * データベースの `docs` テーブルにデータが存在する場合は処理をスキップします。
    *
    * @param src Wikipedia データファイル
    */
  def makeCorpus(namespace:String, src:File, db:Database):Corpus = {
    val corpus = new Corpus(db, namespace)
    val threads = 4
    val executor = Executors.newFixedThreadPool(threads)
    implicit val _context:ExecutionContextExecutor = ExecutionContext.fromExecutor(executor)

    using(new ParallelCaboCha(threads)) { cabocha =>
      val docCount = countLines(src)
      val current = new LongAdder()
      current.add(corpus.paragraphs.size)
      (new Progress(src.getName, corpus.paragraphs.size, docCount)) { prog =>
        (new FileSource(src, gzip = true) :> new TextLine()).drop(1).map { line:String =>
          val Array(id, title, content) = line.split("\t")
          (id.toInt, title, content)
        }.filter { case (id, title, _) =>
          !title.endsWith("一覧") && !title.contains("曖昧さ回避") && !corpus.paragraphs.exists(id)
        }.foreach { case (id, title, content) =>
          prog.report(current.longValue(), f"$title (${content.length}%,d文字)")
          cabocha.tokenize(id, Text.normalize(content)).onComplete {
            case Success(doc) =>
              register(corpus, id, doc)
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
    executor.shutdown()
    corpus
  }

  private[this] def register(corpus:Corpus, id:Int, doc:RelativeDocument[CaboCha.Token]):Unit = {
    val tokens = doc.tokens
    val morphs = tokens.map(_.toMorph)
    val ids = corpus.vocabulary.registerAll(morphs)
    val token2Instance = ids.zip(morphs).zip(tokens).map { case ((morphId, morph), token) =>
      val attr:Map[String, String] = if(token.ne == "*" || token.ne.isEmpty) Map.empty else Map("ne" -> token.ne)
      (token.key, Morph.Instance(morph.surface, morphId, attr))
    }.toMap
    val indexedDoc = doc.replaceTokens(t => token2Instance(t.key))
    corpus.paragraphs.set(id, indexedDoc)
  }

}

object Wikipedia2Corpus {
  private[Wikipedia2Corpus] val logger = LoggerFactory.getLogger(getClass.getName.dropRight(1))

  def main(args:Array[String]):Unit = {
    val src = new File(args(0))
    using(new Database("jdbc:postgresql://localhost/ml-nlp", "postgres", "postgres", "org.postgresql.Driver")) { db =>
      new Wikipedia2Corpus().makeCorpus("", src, db)
      logger.info("コーパスの作成が完了しました。引き続き InitCorpus を実行してください。")
    }
  }
}
