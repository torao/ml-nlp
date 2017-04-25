package at.hazm.ml.nlp.corpus

import at.hazm.ml.nlp.{normalize => nlpNormalize}

object Wikipedia {
  def normalize(content:String):String = {
    // text.replaceAll("""\([ぁ-んァ-ンー、/\s英略称語年月日紀元前男女性\-:;,0-9a-zA-Z・]*\)""", "")
    // Wikipedia の場合、かっこはフリガナや補足のことが多いため削除しても文章の構造上ほぼ問題はない
    nlpNormalize(content).replaceAll("""\(.*?\)""", "")
  }

}
