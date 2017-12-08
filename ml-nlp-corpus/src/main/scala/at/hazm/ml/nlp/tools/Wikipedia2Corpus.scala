package at.hazm.ml.nlp.tools

import java.io.File
import java.util.concurrent.atomic.LongAdder

import at.hazm.core.db.Database
import at.hazm.core.io.using
import at.hazm.core.util.Diagnostics.Progress
import at.hazm.ml.etl.{FileSource, TextLine}
import at.hazm.ml.nlp.ja.{CaboCha, ParallelCaboCha}
import at.hazm.ml.nlp.model.{Morph, POS, RelativeDocument}
import at.hazm.ml.nlp.{Corpus, Text}
import org.slf4j.LoggerFactory

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

object Wikipedia2Corpus {
  private[Wikipedia2Corpus] val logger = LoggerFactory.getLogger(getClass.getName.dropRight(1))

  def transform(corpus:Corpus, id:Int, content:String):RelativeDocument[Morph.Instance] = using(new CaboCha()) { cabocha =>
    retrieve(corpus, cabocha.tokenize(id, Text.normalize(content)))
  }

  /**
    * Extract 済みの Wikipedia 記事データ ([id] [title] [content] の TSV 圧縮形式; 先頭行はヘッダ) に形態素解析と係り受け解析を行い
    * コーパスを作成します。データベースの `docs` テーブルにデータが存在する場合は処理をスキップします。
    *
    * @param src Wikipedia データファイル
    */
  def makeCorpus(namespace:String, src:File, db:Database)(implicit _context:ExecutionContext):Corpus = {
    val corpus = new Corpus(db, namespace)
    val docCount = cache.execIfModified(src) { _ =>
      logger.info("対象文書数を数えています...")
      using(source(src))(_.size)
    }
    if(corpus.documents.size < docCount) {
      using(new ParallelCaboCha(CPUs)) { cabocha =>
        val current = new LongAdder()
        current.add(corpus.documents.size)
        logger.info(f"係り受け解析を開始します: 対象文書 ${docCount - corpus.documents.size}%,d 件")
        (new Progress(src.getName, corpus.documents.size, docCount)) { prog =>
          using(source(src)) { s =>
            val futures = (s :| { case (id, _, _) =>
              !corpus.documents.exists(id)
            }).map { case (id, title, content) =>
              cabocha.tokenize(id, Text.normalize(content)).map { doc =>
                retrieve(corpus, doc, register = true)
                current.increment()
                prog.report(current.longValue(), f"$title (${content.length}%,d文字)")
              }
            }
            Await.ready(Future.sequence(futures), Duration.Inf)
          }
        }
      }
    } else {
      logger.info("Wikipedia")
    }
    logger.info(f"Wikipedia 記事数: $docCount%,d 文書")
    logger.info(f"ドキュメント数: ${corpus.documents.size}%,d 文書")
    logger.info(f"形態素数: ${corpus.vocabulary.size}%,d 単語")
    corpus
  }

  private[this] def retrieve(corpus:Corpus, doc:RelativeDocument[CaboCha.Token], register:Boolean = false):RelativeDocument[Morph.Instance] = {
    val tokens = doc.tokens
    val morphs = tokens.map(_.toMorph)
    val ids = if(register) corpus.vocabulary.registerAll(morphs) else corpus.vocabulary.indicesOf(morphs)

    val idWithToken = ids.zip(tokens)
    val token2Instance:Map[String, Morph.Instance] = idWithToken.map { case (morphId, token) =>
      val instance = if(morphId >= 0) {
        token.toInstance(morphId)
      } else {
        logger.warn(s"未定義の形態素が含まれています: ${token.surface}:${token.pos}")
        POS.values.find(_.default.pos1 == token.pos1) match {
          case Some(unknown) => corpus.vocabulary.defaultInstances(unknown)
          case None =>
            throw new IllegalArgumentException(s"形態素を認識できません: ${token.surface}:${token.pos}")
        }
      }
      (token.key, instance)
    }.toMap
    val instanceDocument = doc.replaceTokens(t => token2Instance(t.key))
    if(register) {
      corpus.documents.set(doc.id, instanceDocument)
    }
    instanceDocument
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
