package at.hazm.ml.nlp.tools

import java.io.File

import at.hazm.core.db.Database
import at.hazm.core.io.using
import org.slf4j.LoggerFactory

/**
  * 形態素解析済みのコーパスに以下の処理を行います。
  *
  * 1. 特徴ベクトルの追加
  * 2. 最短文の分解
  */
object InitCorpus {
  private[this] val logger = LoggerFactory.getLogger(getClass.getName.dropRight(1))

  def main(args:Array[String]):Unit = {
    val src = new File(args(0)).getAbsoluteFile
    val namespace = args.applyOrElse(1, { _:Int => "" })
    logger.info(s"コーパス変換処理を開始します: 名前空間=$namespace, ファイル=$src")
    using(new Database("jdbc:postgresql://localhost/ml-nlp", "postgres", "postgres", "org.postgresql.Driver")) { db =>
      val corpus = Wikipedia2Corpus.makeCorpus(namespace, src, db)
      Morph2FeatureVector.init(corpus, src)
      Paragraph2PerforatedSentence.init(corpus)
    }
  }

}
