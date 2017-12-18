/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements; and to You under the Apache License, Version 2.0.
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package at.hazm.ml.nlp.tools

import java.io.{File, InputStreamReader}
import java.nio.charset.StandardCharsets

import at.hazm.core.db.{Database, _JsonValueType}
import at.hazm.core.io.{CSVIterator, readBinary, readText, using}
import at.hazm.core.util.Diagnostics.Progress
import at.hazm.ml.nlp.ja.Kuromoji
import at.hazm.ml.nlp.{Corpus, Text}
import org.slf4j.LoggerFactory
import play.api.libs.json.{JsArray, JsString}

object QuestionAnswer2Corpus {
  private[QuestionAnswer2Corpus] val logger = LoggerFactory.getLogger(getClass.getName.dropRight(1))

  /**
    * 質問:
    * {{{
    *   question_id     user_id category_id     question_title  question_text   question_datetime
    * }}}
    * 回答:
    * {{{
    *   user_id question_id     answer_text     answer_datetime
    * }}}
    *
    * @param namespace
    * @param q BZIP2
    * @param a BZIP2
    */
  def makeCorpus(db:Database, namespace:String, q:File, a:File):Unit = {
    logger.info(s"making Q&A corpus: $namespace")
    val corpus = new Corpus(db, namespace)

    // 質問を形態素解析して保存
    val questionStore = corpus.newDocumentStore("questions")(_JsonValueType)
    val questionTotal = cache.execIfModified(q)(countLines)
    new Progress("questions", 0, questionTotal) {
      prog =>
      val callback:(Long, Long) => Unit = { (_, line) => prog.report(line) }
      readText(q, bufferSize = 1 * 1024 * 1024, callback = callback) { in =>
        new CSVIterator(in, '\t').drop(1 + questionStore.size).foreach { case Seq(questionId, _, _, _, question, _) =>
          val tokens = Kuromoji.tokenize(Text.normalize(question))
          val morphs = tokens.map(_._1)
          val ids = corpus.vocabulary.registerAll(morphs)
          val json = JsArray(tokens.zip(ids).map { case ((_, instance), id) =>
            JsString(s"$id:${instance.surface}")
          })
          questionStore.set(questionId.toInt, json)
        }
      }
    }
  }

  /**
    * 指定された TSV ファイルのレコード数をカウントします。
    *
    * @param file レコード数をカウントするファイル
    * @return レコード数
    */
  private[this] def countLines(file:File):Int = {
    val step = 50
    val len = file.length()
    var cur = 0
    def callback(bytes:Long, lines:Long):Unit = {
      val value = (bytes * step / len).toInt
      if(cur != value){
        System.out.print("\b" * step + "O" * value + "." * (step - value))
        cur = value
      }
    }
    System.out.print(file.getName + ": " + "." * step)
    val length = readBinary(file, bufferSize = 1 * 1024 * 1024, callback = callback) { is =>
      new CSVIterator(new InputStreamReader(is, StandardCharsets.UTF_8), '\t').length - 1
    }
    System.out.println(f": $length%,d records")
    length
  }

  def main(args:Array[String]):Unit = {
    using(new Database("jdbc:postgresql://localhost/ml-nlp", "postgres", "postgres", "org.postgresql.Driver")) { db =>
      makeCorpus(db, "okwave20171201",
        new File("F:\\ml-nlp\\2017-12-18_questions.tsv.bz2"),
        new File("corpus/okwave/2017-12-18_answers-002.tsv.bz2"))
    }
  }
}
