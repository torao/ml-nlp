/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements; and to You under the Apache License, Version 2.0.
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package at.hazm.ml.nlp.model

import play.api.libs.json._

/**
  * ドキュメントは複数の文を持つ最上位のノードです。
  *
  * @param id        このパラグラフの ID
  * @param sentences このパラグラフの文
  */
case class RelativeDocument[T <: Token](id:Int, sentences:Seq[RelativeSentence[T]]) extends Syntax.AbstractContainerNode[T](sentences) {

  override def toJSON:JsValue = Json.arr(id, super.toJSON)

  /**
    * このドキュメントのトークンを指定された関数を用いて変換した別の種類のトークンに変換します。
    *
    * @param f トークンを変換する関数
    * @tparam R 新しいトークンの型
    * @return 新しいノード
    */
  def replaceTokens[R <: Token](f:(T) => R):RelativeDocument[R] = RelativeDocument(id, sentences.map(_.replaceTokens(f)))

  /**
    * このインスタンスをデバッグ用に文字列化します。
    *
    * @return このインスタンスの文字列
    */
  override def toString:String = sentences.mkString("[", ",", "]")
}

object RelativeDocument {
  def fromJSON[T <: Token](json:JsValue)(implicit _tm:TokenMarshaller[T]):RelativeDocument[T] = {
    json match {
      case JsArray(Seq(JsNumber(id), JsArray(sentences))) =>
        RelativeDocument(id.toInt, sentences.map(s => RelativeSentence.fromJSON(s)(_tm)))
      case unexpected =>
        throw new IllegalArgumentException(s"unexpected json for Document: ${Json.stringify(unexpected)}")
    }
  }
}