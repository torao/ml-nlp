/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements; and to You under the Apache License, Version 2.0.
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package at.hazm.ml.ui

import java.io.File

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.stream.{ActorMaterializer, Materializer}
import at.hazm.core.io.using
import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}
import scala.io.StdIn

object Server {
  private[this] val logger = LoggerFactory.getLogger(getClass.getName.dropRight(1))

  def main(args:Array[String]):Unit = {
    implicit val _system:ActorSystem = ActorSystem()
    implicit val _dispatcher:ExecutionContext = _system.dispatcher
    implicit val _materializer:Materializer = ActorMaterializer()

    val config = ConfigFactory.parseFile(new File(args(0)))

    using(new API(config)) { api =>
      val host = config.getString("http.host")
      val port = config.getInt("http.port")
      val bindingFuture:Future[ServerBinding] = Http().bindAndHandle(api.route, host, port)
      logger.info(s"starting ml-nlp server on: $host:$port")
      logger.info("input ENTER on console to quit")
      StdIn.readLine()
      bindingFuture.map(_.unbind()).onComplete(_ => _system.terminate())
    }
  }
}
