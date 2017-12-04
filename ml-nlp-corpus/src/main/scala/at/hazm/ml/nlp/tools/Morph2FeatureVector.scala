package at.hazm.ml.nlp.tools

import java.io.{File, FileOutputStream, OutputStreamWriter, PrintWriter}
import java.nio.charset.StandardCharsets

import at.hazm.core.io.using
import at.hazm.core.util.Diagnostics.Progress
import at.hazm.ml.nlp.Corpus
import org.deeplearning4j.models.embeddings.loader.WordVectorSerializer
import org.deeplearning4j.models.word2vec.Word2Vec
import org.deeplearning4j.text.sentenceiterator.{LineSentenceIterator, SentenceIterator, SentencePreProcessor}
import org.deeplearning4j.text.tokenization.tokenizerfactory.DefaultTokenizerFactory
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

/**
  * コーパス中の形態素に特徴ベクトルを追加する処理。
  */
object Morph2FeatureVector {
  private[this] val logger = LoggerFactory.getLogger(getClass.getName.dropRight(1))

  def init(corpus:Corpus, file:File):Unit = {

    // Word2Vec のモデルファイルがまだ作成されていなければ作成
    val w2vFile = new File(file.getParentFile, file.getName + ".w2v")
    lazy val w2v = if(!w2vFile.exists()) {

      // SentenceIterator の reset() が何度か呼ばれることで W2V 処理開始まで 30 分程度かかってしまうためテキストファイル化して行う
      val txtFile = new File(file.getParentFile, file.getName + ".paragraphs.txt")
      if(!txtFile.exists()) {
        logger.info(s"exporting paragraph to file: ${txtFile.getName}")
        using(new PrintWriter(new OutputStreamWriter(new FileOutputStream(txtFile), StandardCharsets.UTF_8))) { out =>
          val cursor = corpus.paragraphs.toCursor
          new Progress("export", 0, corpus.paragraphs.size) {
            prog =>
            cursor.foreach { case (id, par) =>
              out.println(par.toIndices.mkString(" "))
              out.flush()
              prog.report(id.toString)
            }
          }
        }
      }

      // Word2Vec 処理の開始
      logger.info(s"creating word2vec model: $txtFile -> ${w2vFile.getName}")
      val it = new LineSentenceIterator(txtFile)
      val w2v = new Word2Vec.Builder()
        .minWordFrequency(5)
        .iterations(1)
        .layerSize(100)
        .seed(42)
        .windowSize(5)
        .iterate(it)
        .tokenizerFactory(new DefaultTokenizerFactory())
        .allowParallelTokenization(false)
        .build()
      w2v.fit()
      WordVectorSerializer.writeWord2VecModel(w2v, w2vFile)
      w2v
    } else {
      WordVectorSerializer.readWord2VecModel(w2vFile)
    }

    // 形態素ごとの特徴ベクトルの保存
    if(corpus.vocabulary.features.size == 0) {
      val tokens = w2v.getVocab.tokens().asScala
      new Progress("save features", 0, tokens.size) {
        prog =>
        tokens.foreach { word =>
          val morphId = word.getWord
          val features = w2v.getWordVector(morphId)
          corpus.vocabulary.features.set(morphId.toInt, features)
          prog.report(word.getWord)
        }
      }
    }

    logger.info(f"paragraphs: ${corpus.paragraphs.size}%,d")
    logger.info(f"vocabulary: ${corpus.vocabulary.size}%,d")
    logger.info(f"featured vocabulary: ${corpus.vocabulary.features.size}%,d")
    logger.info(f"word2vec model paragraph: ${w2v.getVocab.totalNumberOfDocs()}")
    logger.info(f"word2vec model vocabulary: ${w2v.getVocab.numWords()}")

    // Y = (B - A) + X; AにおけるBはXにおけるY
    val A = corpus.vocabulary.instanceOf("日本")
    val B = corpus.vocabulary.instanceOf("韓国")
    val X = corpus.vocabulary.instanceOf("アメリカ")
    for(a <- A; x <- X; b <- B) {
      val ans = w2v.wordsNearest(List(a._1.toString, x._1.toString).asJava, List(b._1.toString).asJava, 20).asScala.flatMap { id =>
        corpus.vocabulary.get(id.toInt).map(m => (id.toInt, m))
      }.filter(_._2.pos1 == "名詞").take(5).map(x => x._2.surface + ":" + x._2.pos).mkString("[", ", ", "]")
      logger.info(s"[${b._2.surface}:${b._2.pos}] - [${a._2.surface}:${a._2.pos}] + [${x._2.surface}:${x._2.pos}] = $ans")
    }
  }

  /**
    * DL4j で文書を列挙するためのラッパークラス。
    *
    * @param corpus コーパス
    */
  private[this] class SI(corpus:Corpus) extends SentenceIterator {
    private[this] var cursor = corpus.paragraphs.toCursor
    private[this] var preProcessor:SentencePreProcessor = _
    private[this] var prog:Option[Progress] = None
    private[this] val max = corpus.paragraphs.size

    override def setPreProcessor(preProcessor:SentencePreProcessor):Unit = this.preProcessor = preProcessor

    override def getPreProcessor:SentencePreProcessor = preProcessor

    override def hasNext:Boolean = cursor.hasNext

    override def nextSentence():String = {
      val (id, doc) = cursor.next()
      prog.getOrElse {
        prog = Some(new Progress("word2vec", 0, max))
        prog.get
      }.report(s"reading doc: $id")
      doc.toIndices.mkString(" ")
    }

    override def reset():Unit = {
      cursor.close()
      cursor = corpus.paragraphs.toCursor
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