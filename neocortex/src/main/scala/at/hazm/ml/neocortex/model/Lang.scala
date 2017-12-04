/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 *  agreements; and to You under the Apache License, Version 2.0.
 *  http://www.apache.org/licenses/LICENSE-2.0
 */
package at.hazm.ml.neocortex.model

import java.io.File
import java.nio.charset.StandardCharsets

import at.hazm.core.db._
import at.hazm.ml.etl.{FileSource, Source, TextLine}
import at.hazm.ml.neocortex.Brain
import at.hazm.core.io.using
import at.hazm.ml.nlp.ja.CaboCha
import org.slf4j.LoggerFactory

/**
  * 言語野。
  */
class Lang private[neocortex](db:Database) {

  /** 形態素とその特徴ベクトル */
  object morphs {
    db.trx { con =>
      con.createTable(s"morphs(id INTEGER NOT NULL PRIMARY KEY AUTO_INCREMENT, morph JSON NOT NULL, features VARCHAR(${20 * 1024}) NOT NULL)")
    }
  }

}

object Lang {
  private[this] val logger = LoggerFactory.getLogger(classOf[Lang])

  def main(args:Array[String]):Unit = {
    val src = new File(args(1))
    val brain = new Brain(new File(args(0)))
    System.out.println(brain.db.trx(_.head("SELECT CURRENT_TIMESTAMP")(_.getTimestamp(1))))

    using(new CaboCha()){ cabocha =>
      wikipedia(src) :> { case (id:Int, doc:String) =>
          cabocha.parse()
      }
    }
  }

  /**
    * 指定された Wikipedia コーパスファイルから文書を読み込む Source を作成します。
    *
    * @param file Wikipedia コーパス
    * @return 文書のソース
    */
  private[this] def wikipedia(file:File):Source[(Int, String)] = {
    new FileSource(file, charset = StandardCharsets.UTF_8, gzip = true) :> new TextLine(skipLines = 1) :> { line:String =>
      val Array(pageId, title, content) = line.split("\t", 3)
      val text = if(content.startsWith(title)) {
        content.substring(title.length).trim
      } else if(title.contains("曖昧さの回避") || title.contains("一覧")) "" else content
      (pageId.toInt, deleteParenthesis(text))
    }
  }

  /**
    * 指定された文字列から括弧に囲まれている範囲を削除します。このメソッドはネストしたカッコに対しても対応する部分で削除を行います。
    *
    * @param text 括弧に囲まれている部分を削除する文字列
    * @return 括弧に囲まれている部分を削除した文字列
    */
  private[this] def deleteParenthesis(text:String):String = {
    var nest = 0
    text.foldLeft(new StringBuilder()) { (buf, ch) =>
      if(ch == '(') nest += 1 else if(nest > 0 && ch == ')') nest -= 1 else if(nest == 0) buf.append(ch)
      buf
    }.toString()
  }

}