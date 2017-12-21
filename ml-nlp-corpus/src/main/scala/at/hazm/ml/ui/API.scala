/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements; and to You under the Apache License, Version 2.0.
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package at.hazm.ml.ui

import java.io.FileNotFoundException
import java.net.URI

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.marshalling.ToResponseMarshallable
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives.{pathPrefix, _}
import akka.http.scaladsl.server.Route
import at.hazm.core.io.{readAllBytes, using}
import at.hazm.ml.ui.API.JsonSupport
import org.slf4j.LoggerFactory
import spray.json._


class API extends JsonSupport {

  import API._

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
          complete(Sentence(sentence.text.reverse))
        }
      case unknown =>
        logger.warn(s"unsupported API: $unknown")
        complete(StatusCodes.NotFound)
    } ~ path(".*".r) { path =>
      logger.warn(s"unsupported POST url: $path")
      complete(StatusCodes.NotFound)
    }
  }

  private[this] def predictFollowingSentence(sentence:Sentence):Sentence = ???

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

  case class Sentence(text:String)

  trait JsonSupport extends SprayJsonSupport with DefaultJsonProtocol {
    implicit val sentenceFormat:RootJsonFormat[Sentence] = jsonFormat1(Sentence)
  }

}
