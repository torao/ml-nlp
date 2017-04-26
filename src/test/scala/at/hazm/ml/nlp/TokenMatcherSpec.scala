package at.hazm.ml.nlp

import at.hazm.ml.nlp.Token.Match
import org.specs2.Specification

class TokenMatcherSpec extends Specification {
  def is =
    s2"""
  The Token.Match class should
    パターンのシーケンスを指定してインスタンス化が可能       $constructor
    形態素シーケンスとの完全一致                          $matches
    形態素シーケンスと前方部分一致                         $indexOf
    形態素シーケンスと後方部分一致                         $lastIndexOf
    最初に一致した部分を置換                              $replaceFirst
    一致した部分をすべて置換                              $replaceAll
                                                      """

  def constructor = {
    Match("私", "私:名詞", ":名詞", "", ":", "*:名詞", "私:*")
    success
  }

  def matches = {
    val me = tk"私".head
    (Match().matches(Seq.empty) must beTrue) and
      (Match(me.term).matches(tk"私") must beTrue) and
      (Match(s"${me.term}:").matches(tk"私") must beTrue) and
      (Match(s"${me.term}:*").matches(tk"私") must beTrue) and
      (Match(s":${me.pos1}").matches(tk"私") must beTrue) and
      (Match(s":${me.pos}").matches(tk"私") must beTrue) and
      (Match(s"*:${me.pos1}").matches(tk"私") must beTrue) and
      (Match(s"*:${me.pos}").matches(tk"私") must beTrue) and
      (Match(s"${me.term}:${me.pos1}").matches(tk"私") must beTrue) and
      (Match(s"${me.term}:${me.pos}").matches(tk"私") must beTrue) and
      (Match(s"${me.term}").matches(tk"あなた") must beFalse) and
      (Match(s"${me.term}").matches(tk"私の鞄") must beFalse) and
      (Match(":").matches(tk":") must beTrue) and
      (Match("::").matches(tk":") must beTrue)
  }

  def indexOf = {
    (Match().indexOf(tk"イギリスの駆逐艦") === 0) and
      (Match("イギリス").indexOf(tk"イギリスの駆逐艦") === 0) and
      (Match("*:名詞-固有名詞-地域", "の:助詞-連体化").indexOf(tk"イギリスの駆逐艦") === 0) and
      (Match("の:助詞-連体化", "駆逐艦").indexOf(tk"イギリスの駆逐艦") === 1) and
      (Match("の:助詞-連体化", "巡洋艦").indexOf(tk"イギリスの駆逐艦") must beLessThan(0)) and
      (Match("*:名詞-固有名詞-地域", "の:助詞-連体化", "駆逐艦", "は").indexOf(tk"イギリスの駆逐艦") must beLessThan(0)) and
      (Match("*:名詞-固有名詞-地域", "の:助詞-連体化").indexOf(tk"イギリスの駆逐艦とインドの巡洋艦", 0) === 0) and
      (Match("*:名詞-固有名詞-地域", "の:助詞-連体化").indexOf(tk"イギリスの駆逐艦とインドの巡洋艦", 1) === 4)
  }

  def lastIndexOf = {
    (Match().lastIndexOf(tk"イギリスの駆逐艦") === 2) and
      (Match("イギリス").lastIndexOf(tk"イギリスの駆逐艦") === 0) and
      (Match("*:名詞-固有名詞-地域", "の:助詞-連体化").lastIndexOf(tk"イギリスの駆逐艦") === 0) and
      (Match("の:助詞-連体化", "駆逐艦").lastIndexOf(tk"イギリスの駆逐艦") === 1) and
      (Match("の:助詞-連体化", "巡洋艦").lastIndexOf(tk"イギリスの駆逐艦") must beLessThan(0)) and
      (Match("*:名詞-固有名詞-地域", "の:助詞-連体化", "駆逐艦", "は").lastIndexOf(tk"イギリスの駆逐艦") must beLessThan(0)) and
      (Match("*:名詞-固有名詞-地域", "の:助詞-連体化").lastIndexOf(tk"イギリスの駆逐艦とインドの巡洋艦", 6) === 4) and
      (Match("*:名詞-固有名詞-地域", "の:助詞-連体化").lastIndexOf(tk"イギリスの駆逐艦とインドの巡洋艦", 3) === 0)
  }

  def replaceFirst = {
    (Match().replaceFirst(tk"イギリスの駆逐艦", _ => Seq(Token("列強", ""))).map(_.term).mkString === "列強イギリスの駆逐艦") and
      (Match("イギリス").replaceFirst(tk"イギリスの駆逐艦", _ => Seq(Token("フランス", ""))).map(_.term).mkString === "フランスの駆逐艦") and
      (Match("*:名詞-固有名詞-地域", "の:助詞-連体化").replaceFirst(tk"イギリスの駆逐艦", _ => Seq(Token("巡洋艦", ""), Token("と", ""))).map(_.term).mkString === "巡洋艦と駆逐艦") and
      (Match("の:助詞-連体化", "駆逐艦").replaceFirst(tk"イギリスの駆逐艦", _ => Seq(Token("の", ""), Token("番組", ""))).map(_.term).mkString === "イギリスの番組") and
      (Match("の:助詞-連体化", "巡洋艦").replaceFirst(tk"イギリスの駆逐艦", _ => Seq(Token("の", ""), Token("番組", ""))).map(_.term).mkString === "イギリスの駆逐艦") and
      (Match("*:名詞-固有名詞-地域", "の:助詞-連体化", "駆逐艦", "は").replaceFirst(tk"イギリスの駆逐艦", _ => Seq(Token("列強", ""))).map(_.term).mkString === "イギリスの駆逐艦")
  }

  def replaceAll = {
    (Match().replaceAll(tk"", _ => Seq.empty) must throwA[IllegalArgumentException]) &&
      (Match("イギリス").replaceAll(tk"イギリスの駆逐艦", _ => Seq(Token("フランス", ""))).map(_.term).mkString === "フランスの駆逐艦") and
      (Match("*:名詞-固有名詞-地域", "の:助詞-連体化").replaceAll(tk"イギリスの駆逐艦", _ => Seq(Token("巡洋艦", ""), Token("と", ""))).map(_.term).mkString === "巡洋艦と駆逐艦") and
      (Match("の:助詞-連体化", "駆逐艦").replaceAll(tk"イギリスの駆逐艦", _ => Seq(Token("の", ""), Token("番組", ""))).map(_.term).mkString === "イギリスの番組") and
      (Match("の:助詞-連体化", "巡洋艦").replaceAll(tk"イギリスの駆逐艦", _ => Seq(Token("の", ""), Token("番組", ""))).map(_.term).mkString === "イギリスの駆逐艦") and
      (Match("*:名詞-固有名詞-地域", "の:助詞-連体化", "駆逐艦", "は").replaceAll(tk"イギリスの駆逐艦", _ => Seq(Token("列強", ""))).map(_.term).mkString === "イギリスの駆逐艦") and
      (Match("で:助動詞", "ある:助動詞", "。:記号-句点").replaceAll(tk"AはBである。ジャガーは自動車メーカーである。", _ => Seq(Token("です", "助動詞"), Token("。", "記号-句点"))).map(_.term).mkString === "AはBです。ジャガーは自動車メーカーです。")
  }
}