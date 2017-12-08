/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements; and to You under the Apache License, Version 2.0.
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package at.hazm.ml.nlp.model

import play.api.libs.json.{JsArray, JsValue, Json}

import scala.annotation.tailrec
import scala.collection.mutable

/**
  * 係り受け解析により分節が関連付けられた文を表すクラス。
  *
  * @param clauses この文に属する文節
  */
case class RelativeSentence[T <: Token](clauses:Seq[RelativeClause[T]]) extends Syntax.AbstractContainerNode[T](clauses) {

  /**
    * このノード内のトークンを指定された関数を用いて変換たノードを生成します。
    *
    * @param f トークンを変換する関数
    * @tparam R 新しいトークンの型
    * @return 新しいノード
    */
  def replaceTokens[R <: Token](f:(T) => R):RelativeSentence[R] = {
    RelativeSentence(clauses.map(_.replaceTokens(f)))
  }

  /**
    * 係り受けのグラフ構造を解析してこの文をより単純化した複数の文に変換します。
    *
    * @return 単純化された文
    */
  def breakdown():Seq[RelativeSentence[T]] = {

    @tailrec
    def _join(clause:RelativeClause[T], map:Map[Int, RelativeClause[T]], buf:mutable.Buffer[RelativeClause[T]] = mutable.Buffer()):Seq[RelativeClause[T]] = {
      buf.append(clause)
      if(clause.link >= 0) _join(map(clause.link), map, buf) else buf
    }

    val map = clauses.groupBy(_.id).mapValues(_.head)
    (map.keySet -- clauses.map(_.link).toSet).toSeq.map(map.apply).map { head =>
      RelativeSentence(_join(head, map))
    }
  }

  /**
    * この文を係り受けや文節などの構造を持たない文に変換します。
    *
    * @return 単純化された文
    */
  def flatten:Sentence[T] = Sentence(tokens)

  /**
    * このインスタンスをデバッグ用に文字列化します。
    *
    * @return このインスタンスの文字列
    */
  override def toString:String = clauses.mkString("{", ",", "}")
}


object RelativeSentence {
  def fromJSON[T <: Token](json:JsValue)(implicit _tm:TokenMarshaller[T]):RelativeSentence[T] = json match {
    case JsArray(arr) =>
      RelativeSentence(arr.map(j => RelativeClause.fromJSON(j)(_tm)))
    case unexpected =>
      throw new IllegalArgumentException(s"unexpected json for RelativeSentence: ${Json.stringify(unexpected)}")
  }
}
