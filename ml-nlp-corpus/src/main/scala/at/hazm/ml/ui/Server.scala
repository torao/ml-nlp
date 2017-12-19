/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements; and to You under the Apache License, Version 2.0.
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package at.hazm.ml.ui

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.model.{ContentTypes, HttpEntity}
import akka.http.scaladsl.server.Directives._
import akka.stream.{ActorMaterializer, Materializer}

import scala.concurrent.{ExecutionContext, Future}
import scala.io.StdIn

object Server {
  def main(args:Array[String]):Unit = {
    implicit val _system:ActorSystem = ActorSystem()
    implicit val _dispatcher:ExecutionContext = _system.dispatcher
    implicit val _materializer:Materializer = ActorMaterializer()

    val route = path("hello") {
      get {
        complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, "<html><body>hello, world</body></html>"))
      }
    }
    val bindingFuture:Future[ServerBinding] = Http().bindAndHandle(route, "localhost", 8800)
    StdIn.readLine()
    bindingFuture.map(_.unbind()).onComplete(_ => _system.terminate())
  }
}
