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

import scala.collection.mutable

/**
  * 質問/回答のような文書セットを形態素解析してコーパスに保存する。
  */
object QuestionAnswer2Corpus {
  private[QuestionAnswer2Corpus] val logger = LoggerFactory.getLogger(getClass.getName.dropRight(1))

  /**
    * 質問ファイルフォーマット:
    * {{{
    *   question_id     user_id category_id     question_title  question_text   question_datetime
    * }}}
    * 回答ファイルフォーマット:
    * {{{
    *   answer_id       user_id question_id     answer_text     answer_datetime
    * }}}
    *
    * @param namespace 作成するコーパスの名前空間
    * @param q         質問ファイル (GZ または BZIP2 可)
    * @param a         回答ファイル (GZ または BZIP2 可)
    */
  def makeCorpus(db:Database, namespace:String, q:File, a:File):Unit = {
    logger.info(s"making Q&A corpus: $namespace")
    val corpus = new Corpus(db, namespace)

    // 質問と回答を形態素解析して保存
    tokenize("question", q, corpus) { row => (row.head.toInt, row(4)) }
    tokenize("answer", a, corpus) { row => (row.head.toInt, row(3)) }

    // 質問と回答の関連付け
    makeRelation(corpus, a)
  }

  private[this] def makeRelation(corpus:Corpus, a:File):Unit = {
    val store = corpus.newRelationStore("question_to_answer")
    val maxQuestionId = store.maxParentId

    // 質問と回答の関係を読み込み
    new Progress("relation (loading)", 0, a.length()).apply { prog =>
      val q2a = mutable.HashMap[Int, mutable.Buffer[Int]]()

      // 質問と回答の関係を保存
      def _flush():Unit = {
        store.register(q2a.toSeq)
        q2a.clear()
      }

      readBinary(a, callback = { (pos:Long) => prog.report(pos) }) { is =>
        new CSVIterator(new InputStreamReader(is), '\t').drop(1).foreach { row =>
          val qid = row(2).toInt
          // 保存中に中断されたものがあるかもしれないので途中から再開するための実装
          if(qid >= maxQuestionId) {
            q2a.getOrElseUpdate(qid, mutable.Buffer[Int]()).append(row.head.toInt)
            if(q2a.size > 10000) {
              _flush()
            }
          }
        }
      }
      _flush()
    }
  }

  private[this] def tokenize(label:String, file:File, corpus:Corpus)(f:Seq[String] => (Int, String)):Unit = {
    val ids = mutable.HashSet[Int]()

    def checkId(id:Int):Unit = if(ids.contains(id)) {
      logger.warn(s"duplicate id detected: $id")
      ids += id
    }

    // 実行統計の取得
    logger.info(s"pre-counting $label articles: $file")
    val store = corpus.newDocumentStore(s"${label}s")(_JsonValueType)
    val total = cache.execIfModified(file)(countArticles)
    val existing = store.size

    logger.info(f"existing ${label}s: $existing%,d / $total%,d articles")
    if(existing < total) readText(file, bufferSize = 1 * 1024 * 1024) { in =>
      logger.info(f"skipping pre-parsed $existing%,d articles")
      val it = new CSVIterator(in, '\t').drop(1)
      if(existing > 0) {
        new Progress(s"${label}s (skipping)", 0, existing) {
          prog =>
          it.take(existing).foreach { x => prog.report(x.last) }
        }
      }
      new Progress(s"${label}s", existing, total) {
        prog =>
        it.foreach { row =>
          val (cid, question) = f(row)
          val tokens = Kuromoji.tokenize(Text.normalize(question))
          val morphs = tokens.map(_._1)
          val ids = corpus.vocabulary.registerAll(morphs)
          val json = JsArray(tokens.zip(ids).map { case ((_, instance), id) =>
            JsString(s"$id:${instance.surface}")
          })
          store.set(cid.toInt, json)
          prog.report(row.last)
        }
      }
    } else {
      logger.info(s"all records already tokenized: $file")
    }
  }

  /**
    * 指定された TSV ファイルのレコード数をカウントします。レコード数にヘッダ行は含まれません。
    *
    * @param file レコード数をカウントするファイル
    * @return レコード数
    */
  private[this] def countArticles(file:File):Int

  = {
    logger.info(s"${file.getName}: counting tsv records")
    val step = 50
    val len = file.length()
    var cur = -1

    def callback(bytes:Long):Unit = {
      val value = (bytes * step / len).toInt
      if(cur != value) {
        logger.info(file.getName + ": " + "O" * value + "." * (step - value) + f": ${bytes / 1024 / 1024}%,dMB / ${file.length() / 1024 / 1024}%,dMB")
        cur = value
      }
    }

    val length = readBinary(file, bufferSize = 1 * 1024 * 1024, callback = callback) { is =>
      new CSVIterator(new InputStreamReader(is, StandardCharsets.UTF_8), '\t').length - 1
    }
    logger.info(f"${file.getName}: $length%,d records")
    length
  }

  def main(args:Array[String]):Unit = {
    using(new Database("jdbc:postgresql://localhost/ml-nlp", "postgres", "postgres", "org.postgresql.Driver")) { db =>
      makeCorpus(db, "okwave20171201",
        new File("F:\\ml-nlp\\2017-12-18_questions.tsv"),
        new File("F:\\ml-nlp\\2017-12-18_answers.tsv"))
    }
  }
}
