/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements; and to You under the Apache License, Version 2.0.
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package at.hazm.ml.nlp.tools

import java.io._
import java.nio.charset.StandardCharsets

import at.hazm.core.io._
import at.hazm.core.util.diag.Stage
import at.hazm.ml.nlp.Text
import at.hazm.ml.nlp.ja.Kuromoji

import scala.util.Try

object XX extends App {
  val entity = "&#(\\d+);".r
  val file = new File("F:\\ml-nlp\\2017-12-19_answers.tsv")
  Stage.exec("回答をトークン化", 0, file.length()) { stage =>
    stage.addOutput(Stage.ConsoleLine)
    readBinary(file, callback = stage.setStepWith) { is =>
      val in = new InputStreamReader(is, StandardCharsets.UTF_8)
      writeText(new File("..\\sample.chainer\\2017-12-19_answers.txt")) { out =>
        new CSVIterator(in, '\t').drop(1).map(c => Try(c.apply(3)).getOrElse(throw new Exception(c.mkString("[", ",", "]")))).foreach { line =>
          entity.replaceAllIn(line, _.group(1).toInt.toChar match {
            case '$' => "\\$"
            case '\\' => "\\\\"
            case ch => ch.toString
          }).split("。").map(_.replace("、", "")).map { text =>
            Kuromoji.tokenize(Text.normalize(text)).map(_._1.surface).filter(s => s.nonEmpty && s != " ").mkString(" ")
          }.filter(_.nonEmpty).foreach(out.println)
        }
      }
    }
  }
}