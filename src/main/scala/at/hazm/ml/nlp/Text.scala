package at.hazm.ml.nlp

import java.text.Normalizer

object Text {

  /**
    * 指定された文字列をテキスト処理に適した形式に変換します。
    *
    * * 指定された文字列の全角英数字を半角に、半角カナ文字を全角カナに変換します。
    *
    * @param text 変換する文字列
    * @return 変換後の文字列
    *
    */
  def normalize(text:String):String = {
    // 連続した空白文字を一つの空白文字に置き換え & 前方空白の除去
    val reduceSpaces = text.foldLeft(new StringBuilder(text.length)){
      case (buffer, ch) if Character.isWhitespace(ch)=>
        if(buffer.nonEmpty && buffer.charAt(buffer.length - 1) != ' '){
          buffer.append(' ')
        } else buffer
      case (buffer, ch)  => buffer.append(ch)
    }
    // 後方空白の除去
    if(reduceSpaces.last == ' '){
      reduceSpaces.setLength(reduceSpaces.length - 1)
    }
    // 大文字に変換
    Normalizer.normalize(reduceSpaces, Normalizer.Form.NFKC).toUpperCase
  }
}
