package at.hazm.ml.nlp.topic

import at.hazm.core.db.{LegacyLocalDB, _}
import at.hazm.ml.nlp.topic.MalletLDA.CorpusTokenizer
import cc.mallet.extract.StringTokenization
import cc.mallet.pipe.{Pipe, SerialPipes, TokenSequence2FeatureSequence}
import cc.mallet.types.Instance

import scala.collection.JavaConverters._

class MalletLDA {

  def fit(db:LegacyLocalDB):Unit = {
    // 学習用のインスタンス (文書) とパイプ (変換処理) を作成
    val pipes = new SerialPipes(Seq(
      new CorpusTokenizer(), // 自前の形態素解析によるトークン化
      new TokenSequence2FeatureSequence() // トークンを ID にマッピング (withBigrams 版もある)
    ).asJava)
  }

}

object MalletLDA {

  private class CorpusTokenizer extends Pipe with Serializable {
    override def pipe(carrier:Instance):Instance = carrier.getData match {
      case db:LegacyLocalDB =>
        val tokens = new StringTokenization("")
        val con = db.newConnection
        val docs = con.query("SELECT content FROM article_morphs")(_.getString(1).split(" ").map(_.toInt).toList).toList
        carrier.setData(docs)
        carrier
      case data =>
        throw new IllegalArgumentException(s"unsupported instance data type: $data")
    }
  }
}