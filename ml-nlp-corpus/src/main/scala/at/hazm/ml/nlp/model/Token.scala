/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 *  agreements; and to You under the Apache License, Version 2.0.
 *  http://www.apache.org/licenses/LICENSE-2.0
 */
package at.hazm.ml.nlp.model

import play.api.libs.json.JsValue

trait Token {
  /** このトークンの表現を参照します。 */
  def surface:String

  /** このトークンを JSON 表現に変換します。 */
  def toJSON:JsValue
}

object Token {

  /**
    * トークンのシーケンスを表す trait。
    */
  trait Sequence {

    /** このシーケンスの トークンを参照します。 */
    def tokens:Seq[Token]

    override def toString:String = tokens.map(_.surface).mkString
  }

}