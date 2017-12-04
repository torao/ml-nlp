package at.hazm.ml.nlp.analytics.phrase

import at.hazm.ml.nlp.Paragraph.Sentence

object Optimizer {

  implicit class _Sentence(sentence:Sentence) {

    /**
      * 指定された文中の括弧に囲まれた部分を削除します。Wikipedia のように、直前の単語の補足や言い換えなどとして使用されているコーパス向け
      * の処理です。
      *
      * @param open  開き括弧を示す形態素ID
      * @param close 閉じ括弧を示す形態素ID
      * @return 括弧が削除された文
      */
    def dropBetween(open:Set[Int], close:Set[Int]):Sentence = {
      var nest = 0
      sentence.copy(clauses = sentence.clauses.map { clause =>
        clause.copy(morphs = clause.morphs.collect {
          case mi if open.contains(mi.morphId) =>
            nest += 1
            None
          case mi if nest > 0 && close.contains(mi.morphId) =>
            nest -= 1
            None
          case mi if nest == 0 =>
            Some(mi)
        }.flatten.toList)
      })
    }
  }

}
