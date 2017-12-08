/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements; and to You under the Apache License, Version 2.0.
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package at.hazm.ml.nlp.model

sealed abstract class POS(val symbol:Char, val label:String) {
  val default:Morph = Morph("*", label, "未定義", "*", "*")
}

object POS {

  val values:Seq[POS] = Seq(
    Noun, Verb, Adjective, AdVerb, PostPositionParticle, Determiner, Continuative, Symbol, Prefix, Filler,
    AuxiliaryVerb, Interjection)

  private[this] val symbol2POS = values.groupBy(_.symbol).mapValues(_.head)
  assert(symbol2POS.size == values.size, s"duplicate POS symbol detected: ${
    values.groupBy(_.symbol).collect {
      case (_, p) if p.length > 1 => p
    }.flatten.mkString("[", ",", "]")
  }")

  /**
    * 指定されたシンボルに対応する品詞を参照します。
    *
    * @param symbol 品詞シンボル
    * @return 品詞
    */
  def valueOf(symbol:Char):POS = symbol2POS(symbol)

  case object Noun extends POS('N', "名詞")

  case object Verb extends POS('V', "動詞")

  case object Adjective extends POS('A', "形容詞")

  case object AdVerb extends POS('E', "副詞")

  case object PostPositionParticle extends POS('P', "助詞")

  case object Determiner extends POS('D', "連体詞")

  case object Continuative extends POS('C', "接続詞")

  case object Symbol extends POS('S', "記号")

  case object Prefix extends POS('X', "接頭詞")

  case object Filler extends POS('F', "フィラー")

  case object AuxiliaryVerb extends POS('U', "助動詞")

  case object Interjection extends POS('I', "感動詞")

  case object Other extends POS('O', "その他")

}
