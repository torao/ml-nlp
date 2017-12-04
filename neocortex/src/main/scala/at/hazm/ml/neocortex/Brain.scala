/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 *  agreements; and to You under the Apache License, Version 2.0.
 *  http://www.apache.org/licenses/LICENSE-2.0
 */
package at.hazm.ml.neocortex

import java.io._

import at.hazm.core.db.Database
import at.hazm.ml.neocortex.model.DataStore
import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.LoggerFactory

class Brain(_dir:File) {

  /**
    * Blob (ファイル) を保存するデータストア。
    */
  val dataStore:DataStore = new DataStore(_dir)

  /**
    * アプリケーションの環境設定。
    */
  val config:Config = ConfigFactory.parseFile(new File(dataStore.dir, "application.conf"))

  /**
    * アプリケーションが使用するデータベース接続。
    */
  val db:Database = {
    val username = config.getString("db.username")
    val password = config.getString("db.password")
    val url = config.getString("db.url")
    val driver = config.getString("db.driver")
    new Database(url = url, username = username, password = password, driver = driver)
  }
}

object Brain {
  private[Brain] val logger = LoggerFactory.getLogger(classOf[Brain])
}