/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements; and to You under the Apache License, Version 2.0.
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package at.hazm.ml.ui

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.stream.ActorMaterializer
import com.typesafe.config.ConfigFactory
import akka.http.scaladsl.server.Directives._

import scala.concurrent.Future

object Server extends RequestTimeout {
  def main(args:Array[String]):Unit = {
    val config = ConfigFactory.load()
    val host = config.getString("host")
    val port = config.getInt("port")

    implicit val _system = ActorSystem()
    implicit val _dispatcher = _system.dispatcher
    implicit val _materializer = ActorMaterializer()

    val api = new RestApi(_system, requestTimeout(config)).route
    val bindingFuture:Future[ServerBinding] = Http().bindAndHandle(api, host, port)
  }
}
