package at.hazm.ml.nlp

import java.text.Normalizer

import at.hazm.ml.nlp.model.Morph

package object ja {

  /**
    * 指定された文字列に対して半角/全角と大文字/小文字の統一を行います。これはテキストを日本語処理向けに正規化する
    * 作業です。
    *
    * @param text 正規化する文字列
    * @return 正規化した文字列
    */
  def normalize(text:String):String = Text.normalize(text)

  /**
    * 指定された文字列を文に分割します。日本語の文は句点を区切りと認識します。
    *
    * @param text 文に分解する文字列
    * @return 文に分解された文字列
    */
  def splitSentence(text:String):Seq[String] = text.split("。").map(_ + "。")

  /**
    * 指定された形態素に含まれるの否定形を直前の動詞または形容詞と結合します (例: [走ら][ない] → [走らない])。
    * この変換処理により真逆の文脈を同一の文脈として重み付けされることを回避できます。
    *
    * @param tokens 否定形を結合する形態素
    */
  def combineNegatives(tokens:Seq[Morph]):Seq[Morph] = tokens.indices.flatMap { i =>
    def neg(t:Morph):Boolean = t.surface == "ない" || t.surface == "なかっ"

    if(i + 1 == tokens.length) Some(tokens(i)) else {
      val (p0, p1) = (tokens(i), tokens(i + 1))
      if((p0.pos1 == "動詞" || p0.pos1 == "形容詞") && neg(p1)) {
        Some(p0.copy(surface = s"${p0.surface}ない"))
      } else if(neg(p0)) {
        None
      } else Some(p0)
    }
  }.toList

}
