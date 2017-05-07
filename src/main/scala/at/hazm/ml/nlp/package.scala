package at.hazm.ml

import java.text.{BreakIterator, Normalizer}

import scala.collection.mutable


package object nlp {

  /**
    * 指定された文字列をテキスト処理に適した形式に変換します。
    *
    * * 指定された文字列の全角英数字を半角に、半角カナ文字を全角カナに変換します。
    *
    * @param text 変換する文字列
    * @return 変換後の文字列
    */
  def normalize(text:String):String = Normalizer.normalize(text.replaceAll("[\\s　]+", " ").trim(), Normalizer.Form.NFKC).toUpperCase

  /**
    * 指定された文章を文 (センテンス) ごとに分割します。
    *
    * @param text センテンスに分割するテキスト
    * @return センテンス
    */
  def splitSentence(text:String):Seq[String] = {
    val it = BreakIterator.getSentenceInstance()
    it.setText(text)
    val buffer = mutable.Buffer[String]()
    (it.first() +: Iterator.continually(it.next()).takeWhile(_ != BreakIterator.DONE).toList).sliding(2).foreach {
      case begin :: end :: Nil =>
        val sentence = text.substring(begin, end).trim()
        if (sentence.nonEmpty) {
          buffer.append(sentence)
        }
      case _ => throw new IllegalStateException()
    }
    buffer
  }

  /**
    * 指定された文章を文 (センテンス) ごとに分割します。
    *
    * @param tokens センテンスに分割するトークン
    * @return センテンス
    */
  def splitSentence(tokens:Seq[Token]):Seq[Seq[Token]] = {
    def _split(buffer:mutable.Buffer[Seq[Token]], begin:Int):Seq[Seq[Token]] = {
      val i = tokens.indexWhere(_.term == "。", begin)
      if(i < 0){
        if(begin < tokens.length - 1){
          buffer.append(tokens.drop(begin))
        }
        buffer
      } else {
        if(begin != i){
          buffer.append(tokens.slice(begin, i+1))
        }
        _split(buffer, i + 1)
      }
    }
    _split(mutable.Buffer(), 0)
  }

  implicit class _TokenParser(val sc:StringContext){
    def tk(args:Any*):Seq[Token] = {
      Token.parse(sc.parts.zip(args).foldLeft(new StringBuilder()){ case (buffer, (str, arg)) =>
        buffer.append(str).append(arg)
      }.append(sc.parts.last).toString())
    }
  }

}
