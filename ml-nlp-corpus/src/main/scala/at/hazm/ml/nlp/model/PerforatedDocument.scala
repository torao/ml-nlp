/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements; and to You under the Apache License, Version 2.0.
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package at.hazm.ml.nlp.model

import at.hazm.ml.nlp.Corpus

case class PerforatedDocument(id:Int, sentences:Seq[Seq[Int]]) {

  /**
    * このインスタンスをデバッグ用に文字列化します。
    *
    * @return このインスタンスの文字列
    */
  def makeString(corpus:Corpus):String = {
    sentences.map { s =>
      s.map(id => corpus.perforatedSentences(id).makeString(corpus.vocabulary)).mkString("[", ",", "]")
    }.mkString(s"$id:", ", ", "")
  }

}
