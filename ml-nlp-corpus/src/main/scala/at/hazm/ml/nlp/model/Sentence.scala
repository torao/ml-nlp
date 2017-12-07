/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements; and to You under the Apache License, Version 2.0.
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package at.hazm.ml.nlp.model

import play.api.libs.json.{JsArray, JsValue, Json}

/**
  * 構造を持たない文を表すクラス。
  *
  * @param tokens この文を表すトークンのシーケンス
  */
case class Sentence[T <: Token](tokens:Seq[T]) extends Syntax.Node[T] {

  /**
    * このノード内のトークンを指定された関数を用いて変換たノードを生成します。
    *
    * @param f トークンを変換する関数
    * @tparam R 新しいトークンの型
    * @return 新しいノード
    */
  def replaceTokens[R <: Token](f:(T) => R):Sentence[R] = {
    Sentence(tokens.map(t => f(t)))
  }

  override def toJSON:JsValue = super.toJSON
}


object Sentence {
  def fromJSON[T <: Token](json:JsValue)(implicit _tm:TokenMarshaller[T]):Sentence[T] = json match {
    case JsArray(arr) => Sentence(arr.map(j => _tm.marshal(j)))
    case unexpected =>
      throw new IllegalArgumentException(s"unexpected json for Sentence: ${Json.stringify(unexpected)}")
  }
}
