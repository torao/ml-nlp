/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements; and to You under the Apache License, Version 2.0.
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package at.hazm.ml.ui

import java.io.{File, FileNotFoundException}
import java.lang.management.ManagementFactory
import java.net.URI
import java.util.concurrent.{Executors, TimeUnit}

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.marshalling.ToResponseMarshallable
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import at.hazm.core.db.Database
import at.hazm.core.io.{readAllBytes, using}
import at.hazm.ml.nlp.Corpus
import at.hazm.ml.nlp.tools.{Paragraph2PerforatedSentence, PerforatedSentence2LSTM, Wikipedia2Corpus}
import at.hazm.ml.ui.API.JsonSupport
import com.typesafe.config.Config
import org.slf4j.LoggerFactory
import spray.json._

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}


class API(conf:Config) extends JsonSupport with AutoCloseable {

  import API._

  private[this] val executor = Executors.newFixedThreadPool(AvailableCPUs)

  private[this] implicit val _context:ExecutionContextExecutor = ExecutionContext.fromExecutor(executor)

  private[this] val db = new Database(conf.getString("corpus.db.url"),
    conf.getString("corpus.db.username"), conf.getString("corpus.db.password"), conf.getString("corpus.db.driver"))

  private[this] lazy val corpus = new Corpus(db, conf.getString("corpus.namespace"))

  private[this] lazy val lstm = PerforatedSentence2LSTM.load(new File(conf.getString("corpus.lstm")))


  def close():Unit = {
    db.close()
    executor.shutdown()
    executor.awaitTermination(10, TimeUnit.SECONDS)
  }

  /** サーバのルーティング定義 */
  val route:Route = get {
    pathPrefix("api/1.0" / Remaining) { path =>
      complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, "<html><body>hello, world</body></html>"))
    } ~ path("hello") {
      complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, "<html><body>hello, world</body></html>"))
    } ~ pathPrefix(Remaining) { path =>
      // 静的ファイルの参照
      val uri = new URI(if(path == "") "index.html" else path).normalize()
      complete(staticFile(uri))
    }
  } ~ post {
    path("api" / "1.0" / ".+".r) {
      case "predict_following_sentences" =>
        entity(as[Sentence]) { sentence =>
          complete(predictFollowingSentence(sentence))
        }
      case unknown =>
        logger.warn(s"unsupported API: $unknown")
        complete(StatusCodes.NotFound)
    } ~ path(".*".r) { path =>
      logger.warn(s"unsupported POST url: $path")
      complete(StatusCodes.NotFound)
    }
  }

  /**
    * 続きの文章を推定します。
    *
    * @param sentence 判定する文章
    * @return 続きの文章
    */
  private[this] def predictFollowingSentence(sentence:Sentence):Sentence = {
    val doc = Paragraph2PerforatedSentence.transform(corpus, Wikipedia2Corpus.transform(corpus, -1, sentence.text))
    val text = if(doc.sentences.nonEmpty) {
      doc.sentences.flatten.map { sentenceId =>
        corpus.perforatedSentences(sentenceId)
      }.foreach { sentence =>
        logger.debug(s">> ${sentence.makeString(corpus.vocabulary)} (${sentence.tokens.mkString(" ")})")
      }
      PerforatedSentence2LSTM.predict(corpus, doc, lstm, 10).map { perforatedId =>
        val sentence = corpus.perforatedSentences.apply(perforatedId)
        val sentenceText = sentence.makeString(corpus.vocabulary)
        logger.debug(s"<< $sentenceText")
        sentenceText
      }.mkString("\n")
    } else ""
    Sentence(text)
  }

  /**
    * 指定された URI から静的ファイルを参照します。これは通常のファイルに対するリクエストと同等です。
    *
    * @param uri 静的ファイルの URI
    * @return 　HTTP レスポンス
    */
  private[this] def staticFile(uri:URI):ToResponseMarshallable = try {
    val content = using(getClass.getClassLoader.getResourceAsStream(s"at/hazm/ml/ui/docroot/$uri"))(in => readAllBytes(in))
    val path = uri.getPath
    val sep = path.lastIndexOf('.')
    val ext = path.substring(sep + 1)
    val media = MediaTypes.forExtension(ext.toLowerCase)
    val contentType = ContentType(media, () => HttpCharsets.`UTF-8`)
    HttpEntity(contentType, content)
  } catch {
    case ex@(_:FileNotFoundException | _:NullPointerException) =>
      logger.warn(s"file not found: $uri: $ex")
      notFound(uri)
  }

  /**
    * 404 Not Found エラーレスポンスを返します。
    *
    * @param uri 404 エラーの URI
    * @return 404 エラーレスポンス
    */
  private[this] def notFound(uri:URI):HttpResponse = {
    val content = s"<html><body><h1>404 Not Found</h1><p>The requested URI <code>$uri</code> is not exist on this server.</p></body></html>"
    HttpResponse(404, entity = HttpEntity(ContentTypes.`text/html(UTF-8)`, content))
  }

}

object API {
  private[API] val logger = LoggerFactory.getLogger(classOf[API])

  private[API] val AvailableCPUs = ManagementFactory.getOperatingSystemMXBean.getAvailableProcessors

  case class Sentence(text:String)

  trait JsonSupport extends SprayJsonSupport with DefaultJsonProtocol {
    implicit val sentenceFormat:RootJsonFormat[Sentence] = jsonFormat1(Sentence)
  }

}
