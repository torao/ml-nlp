/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements; and to You under the Apache License, Version 2.0.
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package at.hazm.ml.nlp

import play.api.libs.json.JsValue

package object model {

  trait TokenMarshaller[T <: Token] {
    def marshal(json:JsValue):T
  }

  implicit object _MorphMarshaller extends TokenMarshaller[Morph] {
    override def marshal(json:JsValue):Morph = Morph.fromJSON(json)
  }

  implicit object _MorphInstanceMarshaller extends TokenMarshaller[Morph.Instance] {
    override def marshal(json:JsValue):Morph.Instance = Morph.Instance.fromJSON(json)
  }

  trait NodeMarshaller[U <: Syntax.Node[_]] {
    def marshal[T <: Token](json:JsValue)(implicit tokenMarshaller:TokenMarshaller[T]):U
  }

  implicit object _ClauseMarshaller extends NodeMarshaller[RelativeClause[_]] {
    override def marshal[T <: Token](json:JsValue)(implicit tokenMarshaller:TokenMarshaller[T]):RelativeClause[T] = {
      RelativeClause.fromJSON(json)(tokenMarshaller)
    }
  }

  implicit object _RelativeSentenceMarshaller extends NodeMarshaller[RelativeSentence[_]] {
    override def marshal[T <: Token](json:JsValue)(implicit tokenMarshaller:TokenMarshaller[T]):RelativeSentence[T] = {
      RelativeSentence.fromJSON(json)(tokenMarshaller)
    }
  }

}
