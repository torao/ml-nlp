/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 *  agreements; and to You under the Apache License, Version 2.0.
 *  http://www.apache.org/licenses/LICENSE-2.0
 */
package at.hazm.ml.nlp.model

import play.api.libs.json.{JsArray, JsValue}

object Syntax {

  /**
    * トークンに分解されたた構文ツリーのノードを表すトレイトです。形態素や文や文章、文節などに該当します。
    */
  trait Node[T <: Token] {
    /** このノードの形態素を参照します。 */
    def tokens:Seq[T]

    /**
      * このノード内のトークンを指定された関数を用いて変換たノードを生成します。
      *
      * @param f トークンを変換する関数
      * @tparam R 新しいトークンの型
      * @return 新しいノード
      */
    def replaceTokens[R <: Token](f:(T) => R):Node[R]

    /** このノードを JSON 形式で返します。 */
    def toJSON:JsValue = JsArray(tokens.map(_.toJSON))

    /** このノードの形態素を文字列化します。 */
    override def toString:String = tokens.map(_.surface).mkString
  }

  /**
    * 構文ツリーのノードを保持する中間層 (枝) に相当するノードです。文や文章、文節などに該当します。
    */
  trait ContainerNode[T <: Token] extends Node[T] {
    /** このノード直下のノードを参照します。 */
    def childNodes:Seq[Node[T]]

    /** このノードの形態素を参照します。 */
    def tokens:Seq[T] = childNodes.flatMap(_.tokens)

    /**
      * このノード内のトークンを指定された関数を用いて変換たノードを生成します。
      *
      * @param f トークンを変換する関数
      * @tparam R 新しいトークンの型
      * @return 新しいノード
      */
    def replaceTokens[R <: Token](f:(T) => R):ContainerNode[R]

    /** このノードを JSON 形式で返します。 */
    override def toJSON:JsValue = JsArray(childNodes.map(_.toJSON))
  }

  abstract class AbstractContainerNode[T <: Token](_childNodes:Seq[Node[T]]) extends ContainerNode[T] {
    override def childNodes:Seq[Node[T]] = _childNodes
  }

}
