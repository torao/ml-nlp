package at.hazm.ml.nlp.tools

import java.io.File

import at.hazm.core.db.LocalDB
import at.hazm.core.io.using
import at.hazm.ml.nlp.Corpus

/**
  * 形態素解析済みのコーパスに以下の処理を行います。
  *
  * 1. 特徴ベクトルの追加
  * 2. 最短文の分解
  */
object InitCorpus {
  def main(args:Array[String]):Unit = {
    val dbFile = new File(args(0)).getAbsoluteFile
    val namespace = args.applyOrElse(1, { _:Int => "" })

    using(new LocalDB(dbFile)) { db =>
      val corpus = new Corpus(db, namespace)
      Morph2FeatureVector.init(corpus, dbFile)
      Paragraph2PerforatedSentence.init(corpus)
    }
  }

}
