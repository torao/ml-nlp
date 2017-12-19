/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements; and to You under the Apache License, Version 2.0.
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package at.hazm.ml.nlp.model

import at.hazm.ml.nlp.corpus.Vocabulary
import at.hazm.ml.nlp.model.PerforatedSentence.{MorphId, Placeholder}

/**
  * プレースホルダ化された文を表すクラスです。
  *
  * @param id     テンプレート文のID
  * @param tokens トークンのシーケンス
  */
case class PerforatedSentence(id:Int, tokens:Seq[PerforatedSentence.Token]) {

  /**
    * 指定されたプレースホルダとコーパスを使用してこのテンプレート文を復元します。
    *
    * @param placeholder プレースホルダ
    * @param vocab       ボキャブラリ
    * @return 復元された文
    */
  def restore(placeholder:Map[Placeholder, Int], vocab:Vocabulary):String = {
    val morphs = vocab.getAll(placeholder.values.toSeq ++ tokens.collect { case MorphId(morphId, _) => morphId })
    val param = placeholder.mapValues(morphId => morphs(morphId))
    tokens.map {
      case MorphId(morphId, _) => morphs(morphId).surface
      case p:Placeholder => param(p).surface
    }.mkString
  }

  /**
    * 指定されたプレースホルダとコーパスを使用してこのテンプレート文をデバッグ用の文字列に変換します。restore() と違い
    * このメソッドで生成される文字列は区切りは範囲の括弧などを含みます。
    *
    * @param vocab       ボキャブラリ
    * @param placeholder プレースホルダ
    * @return 復元された文
    */
  def makeString(vocab:Vocabulary, placeholder:Map[Placeholder, Int] = Map.empty):String = {
    val morphs = vocab.getAll(placeholder.values.toSeq ++ tokens.collect { case MorphId(morphId, _) => morphId })
    val param = placeholder.mapValues(morphId => morphs(morphId))
    tokens.map {
      case m:MorphId => m.toString
      case p@Placeholder(num, pos) => s"${pos.symbol}$num" + param.get(p).map(p => ":" + p.surface).getOrElse("")
    }.map(s => s"[$s]").mkString("(" + (if(id >= 0) s"$id:" else ""), "", ")")
  }
}

object PerforatedSentence {

  sealed trait Token

  case class MorphId(id:Int, surface:String) extends Token {
    override def toString:String = s"$id:$surface"
  }

  object MorphId {
    def fromString(str:String):MorphId = {
      val Array(id, surface) = str.split(":", 2)
      MorphId(id.toInt, surface)
    }
  }

  case class Placeholder(num:Int, pos:POS) extends Token {
    override def toString:String = s"${pos.symbol}$num"
  }

  object Placeholder {
    def fromString(str:String):Placeholder = Placeholder(str.drop(1).toInt, POS.valueOf(str.head))
  }

}
