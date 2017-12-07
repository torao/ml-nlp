package at.hazm.ml.nlp.tools

import java.io.{BufferedReader, File, InputStreamReader}
import java.lang.management.ManagementFactory
import java.util.concurrent.{Executors, TimeUnit}

import at.hazm.core.db.Database
import at.hazm.core.io.using
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}

/**
  * 形態素解析済みのコーパスに以下の処理を行います。
  *
  * 1. 特徴ベクトルの追加
  * 2. 最短文の分解
  */
object InitCorpus {
  private[this] val logger = LoggerFactory.getLogger(getClass.getName.dropRight(1))

  def main(args:Array[String]):Unit = {
    val threads = ManagementFactory.getOperatingSystemMXBean.getAvailableProcessors - 2
    val executor = Executors.newFixedThreadPool(threads)
    implicit val _context:ExecutionContextExecutor = ExecutionContext.fromExecutor(executor)

    val src = new File(args(0)).getAbsoluteFile
    val namespace = args.applyOrElse(1, { _:Int => "" })
    logger.info(s"コーパス変換処理を開始します: 名前空間=$namespace, ファイル=$src")
    using(new Database("jdbc:postgresql://localhost/ml-nlp", "postgres", "postgres", "org.postgresql.Driver")) { db =>
      val corpus = Wikipedia2Corpus.makeCorpus(namespace, src, db)
      Morph2FeatureVector.init(corpus, namespace)
      Paragraph2PerforatedSentence.init(corpus)
      val lstm = PerforatedSentence2LSTM.train(corpus, namespace)

      // LSTM 予測の実行
      val in = new BufferedReader(new InputStreamReader(System.in))
      val buffer = new StringBuilder()

      def _loop():Unit = {
        System.out.print(">> ")
        val text = in.readLine()
        if(text != null && !text.equalsIgnoreCase("quit")) {
          if(text.trim().nonEmpty){
            try {
              val doc = Paragraph2PerforatedSentence.transform(corpus, Wikipedia2Corpus.transform(corpus, -1, text))
              if(doc.sentences.nonEmpty){
                PerforatedSentence2LSTM.predict(corpus, doc, lstm, 10).map { perforatedId =>
                  val sentence = corpus.perforatedSentences.apply(perforatedId)
                  sentence.mkString(corpus)
                }.foreach(s => println(s"<< $s"))
                buffer.append(text)
              } else println("<< 認識できる文が含まれていませんでした。")
            } catch {
              case ex:Throwable =>
                ex.printStackTrace()
            }
          }
          _loop()
        }
      }

      logger.info("LSTM 文章予測インタラクティブ・シェルを開始しています (quit で終了)")
      _loop()
    }

    executor.shutdown()
    executor.awaitTermination(10, TimeUnit.SECONDS)
  }

}
