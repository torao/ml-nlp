package at.hazm.ml.nlp.tools

import java.io.File

import at.hazm.core.util.Diagnostics.Progress
import at.hazm.ml.nlp.Corpus
import org.deeplearning4j.models.embeddings.loader.WordVectorSerializer
import org.deeplearning4j.models.word2vec.Word2Vec
import org.deeplearning4j.text.sentenceiterator.{SentenceIterator, SentencePreProcessor}
import org.deeplearning4j.text.tokenization.tokenizerfactory.DefaultTokenizerFactory
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

object Morph2FeatureVector {
  private[this] val logger = LoggerFactory.getLogger(getClass.getName.dropRight(1))

  /**
    * 形態素解析と係り受け解析が行われたコーパスの書く形態素に対して Word2Vec で特徴ベクトルを算出する処理。特徴ベクトルが付与される
    * 形態素はある数以上出現していなければならない。
    *
    * @param corpus   コーパス
    * @param baseName 生成ファイルのベース名 (W2Vパラメータや拡張子が付与されモデルの出力ファイル名となる)
    */
  def init(corpus:Corpus, baseName:String):File = {
    logger.info(s"--- 形態素の特徴ベクトル化 (Word2Vec) ---")

    // パラメータ設定
    val minWordFrequency = 5
    val iteration = 1
    val layerSize = 100
    val windowSize = 5
    val learningRate = 0.01

    // Word2Vec のモデルファイルがまだ作成されていなければ作成
    val file = new File(s"${baseName}_d${corpus.documents.size}w${corpus.vocabulary.size}l${layerSize}i${iteration}w${windowSize}l${(learningRate * 100).toInt}mf${minWordFrequency}-word2vec.zip")
    lazy val w2v = if(!file.exists()) {

      // Word2Vec 処理の開始
      val it = new SI(corpus)
      val w2v = new Word2Vec.Builder()
        .minWordFrequency(minWordFrequency)
        .iterations(iteration)
        .layerSize(layerSize)
        .seed(42)
        .windowSize(windowSize)
        .learningRate(learningRate)
        .iterate(it)
        .tokenizerFactory(new DefaultTokenizerFactory())
        .allowParallelTokenization(false)
        .build()
      w2v.fit()
      WordVectorSerializer.writeWord2VecModel(w2v, file)
      corpus.vocabulary.features.deleteAll()
      w2v
    } else {
      logger.info(s"Word2Vec モデルがすでに存在します: $file")
      WordVectorSerializer.readWord2VecModel(file)
    }

    // 形態素ごとの特徴ベクトルの保存
    val tokens = w2v.getVocab.tokens().asScala
    val existingSize = corpus.vocabulary.features.size
    if(existingSize < tokens.size) {
      logger.info(s"形態素の特徴ベクトルを保存しています: $existingSize < ${tokens.size}")
      new Progress("save features", existingSize, tokens.size) {
        prog =>
        tokens.drop(existingSize).foreach { word =>
          val morphId = word.getWord
          val features = w2v.getWordVector(morphId)
          corpus.vocabulary.features.set(morphId.toInt, features)
          prog.report(word.getWord)
        }
      }
    }

    logger.info(f"文書数: ${corpus.documents.size}%,d")
    logger.info(f"形態素数: ${corpus.vocabulary.size}%,d")
    logger.info(f"特徴ベクトルを付与した形態素数: ${corpus.vocabulary.features.size}%,d")
    logger.info(f"Word2Vec モデル中の文書数: ${w2v.getVocab.totalNumberOfDocs()}")
    logger.info(f"Word2Vec モデル中の単語数: ${w2v.getVocab.numWords()}")

    // 最も近い文字
    val A = corpus.vocabulary.instanceOf("日本")
    logger.info(s">> [日本] => " +
      w2v.wordsNearest(A.head._1.toString, 5).asScala
        .map(m => corpus.vocabulary(m.toInt)).map(m => m.surface + ":" + m.pos).mkString("[", ", ", "]")
    )

    // Y = (B - A) + X; AにおけるBはXにおけるY
    val B = corpus.vocabulary.instanceOf("韓国")
    val X = corpus.vocabulary.instanceOf("アメリカ")
    for(a <- A; x <- X; b <- B) {
      val ans = w2v.wordsNearest(List(a._1.toString, x._1.toString).asJava, List(b._1.toString).asJava, 20).asScala.flatMap { id =>
        corpus.vocabulary.get(id.toInt).map(m => (id.toInt, m))
      }.filter(_._2.pos1 == "名詞").take(5).map(x => x._2.surface + ":" + x._2.pos).mkString("[", ", ", "]")
      logger.info(s">> [${b._2.surface}:${b._2.pos}] - [${a._2.surface}:${a._2.pos}] + [${x._2.surface}:${x._2.pos}]")
      logger.info(s">>   = $ans")
    }

    file
  }

  /**
    * DL4j で文書を列挙するためのラッパークラス。
    *
    * @param corpus コーパス
    */
  private[this] class SI(corpus:Corpus) extends SentenceIterator {
    private[this] var cursor = corpus.documents.toCursor
    private[this] var preProcessor:SentencePreProcessor = _
    private[this] var prog:Option[Progress] = None
    private[this] val max = corpus.documents.size

    override def setPreProcessor(preProcessor:SentencePreProcessor):Unit = this.preProcessor = preProcessor

    override def getPreProcessor:SentencePreProcessor = preProcessor

    override def hasNext:Boolean = cursor.hasNext

    override def nextSentence():String = {
      val (id, doc) = cursor.next()
      prog.getOrElse {
        prog = Some(new Progress("word2vec", 0, max))
        prog.get
      }.report(s"reading doc: $id")
      doc.tokens.map(_.morphId).mkString(" ")
    }

    override def reset():Unit = {
      cursor.close()
      cursor = corpus.documents.toCursor
      prog.foreach { p =>
        p.report(max)
        prog = None
      }
    }

    override def finish():Unit = {
      cursor.close()
      prog.foreach(_.report(max))
    }
  }

}