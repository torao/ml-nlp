/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 *  agreements; and to You under the Apache License, Version 2.0.
 *  http://www.apache.org/licenses/LICENSE-2.0
 */

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements; and to You under the Apache License, Version 2.0.
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package at.hazm.ml.nlp.model

import play.api.libs.json._

/**
  * 係り受け解析により関係が示された文節を表すクラス。
  *
  * @param id     文節 ID (文の中で一意)
  * @param link   係り先の文節 ID (係り受け先がない場合は負の値)
  * @param rel    係り受け関係
  * @param score  係り受けの強さ (大きい方がかかりやすい)
  * @param head   主辞の形態素 ID
  * @param func   機能語の形態素 ID
  * @param tokens この文節の形態素
  */
case class RelativeClause[T <: Token](id:Int, link:Int, rel:String, score:Double, head:Int, func:Int, tokens:Seq[T]) extends Syntax.Node[T] {

  /**
    * このノード内のトークンを指定された関数を用いて変換たノードを生成します。
    *
    * @param f トークンを変換する関数
    * @tparam R 新しいトークンの型
    * @return 新しいノード
    */
  def replaceTokens[R <: Token](f:(T) => R):RelativeClause[R] = {
    RelativeClause(id, link, rel, score, head, func, tokens.map(f))
  }

  // link="-1" rel="D" score="0.000000" head="12" func="13"
  override def toJSON:JsValue = Json.arr(id, super.toJSON, link, score, rel, head, func)

  /**
    * このインスタンスをデバッグ用に文字列化します。
    *
    * @return このインスタンスの文字列
    */
  override def toString:String = tokens.map(_.surface).mkString("(", ".", ")")
}

object RelativeClause {
  def fromJSON[T <: Token](json:JsValue)(implicit _tm:TokenMarshaller[T]):RelativeClause[T] = json match {
    case JsArray(Seq(JsNumber(id), JsArray(tokens), JsNumber(link), JsNumber(score), JsString(rel), JsNumber(head), JsNumber(func))) =>
      RelativeClause(id.toInt, link.toInt, rel, score.toDouble, head.toInt, func.toInt, tokens.map(t => _tm.marshal(t)))
    case unexpected =>
      throw new IllegalArgumentException(s"unexpected json for RelativeSentence: ${Json.stringify(unexpected)}")
  }
}