package at.hazm.ml.nlp.tools

import java.io.File
import java.lang.management.ManagementFactory
import java.util.concurrent.atomic.LongAdder
import java.util.concurrent.{Executors, TimeUnit}

import at.hazm.core.db.Database
import at.hazm.core.io.using
import at.hazm.core.util.Diagnostics.Progress
import at.hazm.ml.etl.{FileSource, TextLine}
import at.hazm.ml.nlp.ja.{CaboCha, ParallelCaboCha}
import at.hazm.ml.nlp.model.{Morph, RelativeDocument}
import at.hazm.ml.nlp.{Corpus, Text}
import org.slf4j.LoggerFactory

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}

/**
  * @deprecated ml-nlp-extract を使用してください。
  */
object Wikipedia2Corpus {
  private[Wikipedia2Corpus] val logger = LoggerFactory.getLogger(getClass.getName.dropRight(1))

  /**
    * 指定された Extract 済み Wikipedia データ (id title content の TSV 圧縮形式; 先頭行はヘッダ) からコhgーパスを作成します。
    * データベースの `docs` テーブルにデータが存在する場合は処理をスキップします。
    *
    * @param src Wikipedia データファイル
    */
  def makeCorpus(namespace:String, src:File, db:Database):Corpus = {
    val corpus = new Corpus(db, namespace)
    val docCount = cache.execIfModified(src) { _ =>
      logger.info("対象文書数を数えています...")
      using(source(src))(_.size)
    }
    if(corpus.documents.size < docCount) {
      val threads = ManagementFactory.getOperatingSystemMXBean.getAvailableProcessors
      val executor = Executors.newFixedThreadPool(threads)
      implicit val _context:ExecutionContextExecutor = ExecutionContext.fromExecutor(executor)

      using(new ParallelCaboCha(threads)) { cabocha =>
        val current = new LongAdder()
        current.add(corpus.documents.size)
        logger.info(f"係り受け解析を開始します: 対象文書 ${docCount - corpus.documents.size}%,d 件")
        (new Progress(src.getName, corpus.documents.size, docCount)) { prog =>
          using(source(src)) { s =>
            val futures = (s :| { case (id, _, _) =>
              !corpus.documents.exists(id)
            }).map { case (id, title, content) =>
              cabocha.tokenize(id, Text.normalize(content)).map { doc =>
                register(corpus, id, doc)
                current.increment()
                prog.report(current.longValue(), f"$title (${content.length}%,d文字)")
              }
            }
            Await.ready(Future.sequence(futures), Duration.Inf)
          }
        }
      }
      executor.shutdown()
      executor.awaitTermination(10, TimeUnit.SECONDS)
    }
    logger.info(f"Wikipedia 記事数: $docCount%,d 文書")
    logger.info(f"ドキュメント数: ${corpus.documents.size}%,d 文書")
    logger.info(f"形態素数: ${corpus.vocabulary.size}%,d 単語")
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
    corpus.documents.set(id, indexedDoc)
  }

  private[this] def source(src:File) = {
    new FileSource(src, gzip = true) :> new TextLine(skipLines = 1) :> { line:String =>
      val Array(id, title, content) = line.split("\t")
      (id.toInt, title, content)
    } :| { case (id, title, _) =>
      !title.endsWith("一覧") && !title.contains("曖昧さ回避")
    }
  }

}
