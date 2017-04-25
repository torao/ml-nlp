package at.hazm.ml

import java.text.Normalizer


package object nlp {

  /**
    * 指定された文字列をテキスト処理に適した形式に変換します。
    *
    * * 指定された文字列の全角英数字を半角に、半角カナ文字を全角カナに変換します。
    *
    * @param text 変換する文字列
    * @return 変換後の文字列
    */
  def normalize(text:String):String = Normalizer.normalize(text.replaceAll("\\s+", " ").trim(), Normalizer.Form.NFKC).toUpperCase

}
