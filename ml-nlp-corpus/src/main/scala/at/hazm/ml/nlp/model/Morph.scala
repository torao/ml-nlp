/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 *  agreements; and to You under the Apache License, Version 2.0.
 *  http://www.apache.org/licenses/LICENSE-2.0
 */
package at.hazm.ml.nlp.model

import at.hazm.ml.nlp.model.Morph.AbstractMorph
import play.api.libs.json._

import scala.collection.mutable

/**
  * 形態素を現すクラスです。
  * IPADIC をベースとした形態素解析と同等のプロパティを持ちます。
  *
  * @param surface 表現の基本形
  * @param pos1    品詞レベル 1 (名詞, 助詞, 動詞, 感嘆詞, etc.)
  * @param pos2    品詞レベル 2 (一般, 連体化, 係助詞, 自立, etc.)
  * @param pos3    品詞レベル 3 (組織, 一般, etc.)
  * @param pos4    品詞レベル 4
  * @see https://github.com/atilika/kuromoji/blob/master/kuromoji-ipadic/src/main/java/com/atilika/kuromoji/ipadic/Token.java
  */
case class Morph(surface:String, pos1:String, pos2:String, pos3:String, pos4:String) extends AbstractMorph {

  /**
    * 形態素の同一性を比較するためのキーを参照します。このキーが一致する形態素は同一とみなすことができます。
    *
    * @return 同一性のキー
    */
  def key:String = s"$surface:$pos"

  /**
    * この形態素の内容を JSON 形式に変換します。
    *
    * @return JSON 表現
    */
  def toJSON:JsValue = Json.arr(surface, Json.arr(pos1, pos2, pos3, pos4))
}

object Morph {

  /**
    * 指定された JSON 表現から形態素情報を復元します。
    *
    * @param json 形態素を復元する JSON
    * @return 復元した形態素
    */
  def fromJSON(json:JsValue):Morph = json match {
    case JsArray(Seq(JsString(surface), JsArray(Seq(JsString(pos1), JsString(pos2), JsString(pos3), JsString(pos4))))) =>
      Morph(surface, pos1, pos2, pos3, pos4)
    case unsupported =>
      throw new IllegalArgumentException(s"unsupported morph form: ${Json.stringify(unsupported)}")
  }

  /**
    * 形態素を表すクラスです。
    * IPADIC をベースとした形態素解析と同等のプロパティを持ちます。
    *
    * @see https://github.com/atilika/kuromoji/blob/master/kuromoji-ipadic/src/main/java/com/atilika/kuromoji/ipadic/Token.java
    */
  abstract class AbstractMorph extends Token {
    /** 表現の基本形 */
    def surface:String

    /** 品詞レベル 1 (名詞, 助詞, 動詞, 感嘆詞, etc.) */
    def pos1:String

    /** 品詞レベル 2 (一般, 連体化, 係助詞, 自立, etc.) */
    def pos2:String

    /** 品詞レベル 3 (組織, 一般, etc.) */
    def pos3:String

    /** 品詞レベル 4 */
    def pos4:String

    /**
      * この形態素の品詞をハイフンで連結した文字列です。
      */
    lazy val pos:String = Seq(pos1, pos2, pos3, pos4).reverse.dropWhile(_.isEmpty).reverse.mkString("-")

    /**
      * 形態素の同一性を比較するためのキーを参照します。このキーが一致する形態素は同一とみなすことができます。
      *
      * @return 同一性のキー
      */
    def key:String

  }

  /**
    * 文章中に存在する形態素のインスタンスです。実際に文章中での表現とその属性を持ちます。
    *
    * @param morphId         形態素ID
    * @param surface         形態素のインスタンス表現
    * @param conjugationType 活用型
    * @param conjugationForm 活用形
    * @param reading         読み
    * @param pronunciation   発音
    * @param attr            属性
    */
  case class Instance(morphId:Int, surface:String, conjugationType:String, conjugationForm:String, reading:String, pronunciation:String, attr:Map[String, String]) extends Token {
    /** このシーケンスの形態素を参照します。 */
    def morphs:Seq[Instance] = Seq(this)

    /**
      * この形態素インスタンスを JSON で表現します。
      */
    def toJSON:JsValue = {
      val base = Json.arr(morphId, surface, conjugationType, conjugationForm, reading, pronunciation)
      if(attr.isEmpty) base else base :+ JsObject(attr.mapValues(s => JsString(s)))
    }
  }

  object Instance {
    def fromJSON(json:JsValue):Instance = json match {
      case JsArray(Seq(JsNumber(morphId), JsString(surface), JsString(conjugationType), JsString(conjugationForm), JsString(reading), JsString(pronunciation), JsObject(attr))) =>
        Instance(morphId.toInt, surface, conjugationType, conjugationForm, reading, pronunciation, attr.mapValues(_.as[String]).toMap)
      case JsArray(Seq(JsNumber(morphId), JsString(surface), JsString(conjugationType), JsString(conjugationForm), JsString(reading), JsString(pronunciation))) =>
        Instance(morphId.toInt, surface, conjugationType, conjugationForm, reading, pronunciation, Map.empty)
      case unsupported =>
        throw new IllegalArgumentException(s"unsupported morph instance form: ${Json.stringify(unsupported)}")
    }
  }

  /**
    * 形態素と一致判定を行うパターン。
    */
  private[nlp] class Pattern(term:Option[String], pos:Option[String]) {
    /** 指定された形態素と一致する場合 true */
    def matches(token:Morph):Boolean = {
      term.forall(_ == token.surface) && pos.forall(p => p == token.pos1 || token.pos1.startsWith(p + "-"))
    }

    override def toString:String = s"${term.getOrElse("*")}:${pos.getOrElse("*")}"
  }

  /**
    * 形態素解析した Morph のシーケンスと部分一致を判定するためのクラスです。パターンには `"私"` のような入力単語 (surface 部分のみ)
    * または `"私:名詞"` のような品詞付きの文字列が使用できます。入力単語と品詞はいずれもワイルドカードを示す `"私:"` もしくは `"私:*"`
    * を使用することができます。ただし `":"` や `"::"` はワイルドカードではなく記号としてのコロンと解釈されます。
    *
    * @param termsWithPOS パターンの列挙
    */
  case class Match(termsWithPOS:String*) {

    /** 一致判定用のパターン */
    private[this] val pattern:Seq[Pattern] = termsWithPOS.map { twp =>
      if(twp == ":") {
        new Pattern(Some(":"), None)
      } else if(twp.startsWith("::")) {
        val pos = twp.substring(2)
        new Pattern(Some(":"), if(pos.isEmpty || pos == "*") None else Some(pos))
      } else twp.split(":", 2) match {
        case Array(term, pos) =>
          new Pattern(if(term.isEmpty || term == "*") None else Some(term), if(pos.isEmpty || pos == "*") None else Some(pos))
        case Array(term) =>
          new Pattern(if(term.isEmpty || term == "*") None else Some(term), None)
      }
    }

    override def toString:String = pattern.map(_.toString.replaceAll("\"", "\"\"")).mkString("[\"", "\",\"", "\"]")

    /**
      * 指定された形態素シーケンス全体がこのパターンと一致しているか判定します。
      *
      * @param tokens 判定する形態素シーケンス
      * @return 全体が一致している場合 true
      */
    def matches(tokens:Seq[Morph]):Boolean = tokens.length == pattern.length && indexOf(tokens) == 0

    /**
      * 指定された形態素シーケンスの前方から {{{begin}}} 以降でこのパターンと一致する部分を参照します。一致する部分が存在しない場合は
      * 負の値を返します。
      *
      * @param tokens 判定する形態素シーケンス
      * @param begin  判定を開始する位置
      * @return 部分一致している位置、または負の値
      */
    def indexOf(tokens:Seq[Morph], begin:Int = 0):Int = if(tokens.length - begin < pattern.length) -1 else {
      (begin to (tokens.length - pattern.length)).find { i =>
        pattern.indices.forall(j => pattern(j).matches(tokens(i + j)))
      }.getOrElse(-1)
    }

    def contains(tokens:Seq[Morph]):Boolean = indexOf(tokens) >= 0

    /**
      * 指定された形態素シーケンスの後方から {{{begin}}} 以前でこのパターンと一致する部分を参照します。一致する部分が存在しない場合は
      * 負の値を返します。
      *
      * @param tokens 判定する形態素シーケンス
      * @param begin  判定を開始する位置
      * @return 部分一致している位置、または負の値
      */
    def lastIndexOf(tokens:Seq[Morph], begin:Int = -1):Int = {
      val _begin = if(begin < 0) tokens.length - 1 else begin
      if(tokens.length - (tokens.length - _begin) < pattern.length) -1 else {
        (math.min(_begin, tokens.length - pattern.length) to 0 by -1).find { i =>
          pattern.indices.forall(j => pattern(j).matches(tokens(i + j)))
        }.getOrElse(-1)
      }
    }

    /**
      * 指定された形態素シーケンスがこの Match で開始しているかを判定します。
      *
      * @param tokens 先頭一致を判定する形態素シーケンス
      * @return 先頭が一致している場合 true
      */
    def startsWith(tokens:Seq[Morph]):Boolean = if(tokens.length < pattern.length) false else {
      pattern.indices.forall(i => pattern(i).matches(tokens(i)))
    }

    /**
      * 指定された形態素シーケンスがこの Match で終了しているかを判定します。
      *
      * @param tokens 末尾一致を判定する形態素シーケンス
      * @return 末尾が一致している場合 true
      */
    def endsWith(tokens:Seq[Morph]):Boolean = if(tokens.length < pattern.length) false else {
      pattern.indices.forall(i => pattern(i).matches(tokens(i)))
    }

    /**
      * 指定された形態素のシーケンスのからこの Match と最初に一致する部分を {{{convert}}} が返すシーケンスに置き換えます。
      *
      * @param tokens  パターン置き換えを行うシーケンス
      * @param convert 置き換え後の形態素を決定する関数
      * @return 置き換えを行った形態素シーケンス
      */
    def replaceFirst(tokens:Seq[Morph], convert:(Seq[Morph]) => Seq[Morph]):Seq[Morph] = if(tokens.length < pattern.length) tokens else {
      val i = indexOf(tokens)
      if(i >= 0) {
        tokens.take(i) ++ convert(tokens.slice(i, i + pattern.length)) ++ tokens.drop(i + pattern.length)
      } else tokens
    }

    /**
      * 指定された形態素のシーケンスのからこの Match と一致するすべての部分を {{{convert}}} が返すシーケンスに置き換えます。
      *
      * @param tokens  パターン置き換えを行うシーケンス
      * @param convert 置き換え後の形態素を決定する関数
      * @return 置き換えを行った形態素シーケンス
      */
    def replaceAll(tokens:Seq[Morph], convert:(Seq[Morph]) => Seq[Morph]):Seq[Morph] = if(tokens.length < pattern.length) tokens else {
      if(pattern.isEmpty) {
        throw new IllegalArgumentException("replaceAll() cannot use for empty pattern")
      }
      notMatches(tokens).foldLeft(mutable.Buffer[Morph]()) { case (buffer, (begin, end)) =>
        buffer.appendAll(tokens.slice(begin, end))
        if(end < tokens.length) {
          val part = tokens.slice(end, end + pattern.length)
          buffer.appendAll(convert(part))
        }
        buffer
      }
    }

    def split(tokens:Seq[Morph]):Seq[Seq[Morph]] = {
      if(pattern.isEmpty) {
        throw new IllegalArgumentException("split() cannot use for empty pattern")
      }
      notMatches(tokens).foldLeft(mutable.Buffer[Seq[Morph]]()) { case (buffer, (begin, end)) =>
        buffer.append(tokens.slice(begin, end))
        buffer
      }
    }

    private[this] def notMatches(tokens:Seq[Morph], fromIndex:Int = 0):Stream[(Int, Int)] = {
      val i = indexOf(tokens, fromIndex)
      if(i < 0) {
        (fromIndex, tokens.length) #:: Stream.empty[(Int, Int)]
      } else {
        (fromIndex, i) #:: notMatches(tokens, i + pattern.length)
      }
    }
  }

}