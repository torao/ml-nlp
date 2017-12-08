/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements; and to You under the Apache License, Version 2.0.
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package at.hazm.ml.nlp.model

import at.hazm.ml.nlp.Corpus
import at.hazm.ml.nlp.model.PerforatedSentence.{MorphId, Placeholder}

case class PerforatedSentence(id:Int, tokens:Seq[PerforatedSentence.Token]) {
  def mkString(corpus:Corpus):String = tokens.map {
    case MorphId(morphId) => corpus.vocabulary(morphId).surface
    case Placeholder(_, pos) => s"[${pos.symbol}]"
  }.mkString(" ")
}

object PerforatedSentence {

  sealed trait Token

  case class MorphId(id:Int) extends Token {
    override def toString:String = id.toString
  }

  object MorphId {
    def fromString(str:String):MorphId = MorphId(str.toInt)
  }

  case class Placeholder(num:Int, pos:POS) extends Token {
    override def toString:String = s"${pos.symbol}$num"
  }

  object Placeholder {
    def fromString(str:String):Placeholder = Placeholder(str.drop(1).toInt, POS.valueOf(str.head))
  }

}
