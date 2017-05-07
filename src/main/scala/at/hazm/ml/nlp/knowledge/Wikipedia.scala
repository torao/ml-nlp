package at.hazm.ml.nlp.knowledge

import at.hazm.ml.nlp.{Token, normalize, splitSentence}

import scala.collection.mutable


case object Wikipedia extends Source("wikipedia", "Wikipedia") {

  /**
    * 指定されて文字列から丸括弧で囲まれている部分を削除します。このメソッドは括弧がネストしている場合にもすべての階層を削除します。
    * Wikipedia には「赤塚 不二夫（あかつか ふじお、本名：赤塚 藤雄、1935年（昭和10年）9月14日 - 2008年（平成20年）8月2日）は、
    * 日本の漫画家。」のようにネストした丸括弧が含まれています。
    *
    * @param text 括弧内を除去する文字列
    * @return 括弧内を除去した文字列
    */
  def deleteParenthesis(text:String):String = {
    val buffer = new StringBuilder()
    var nest = 0
    for(i <- text.indices) {
      text(i) match {
        case '(' | '（' => nest += 1
        case ')' | '）' => nest = math.max(0, nest - 1)
        case ch =>
          if(nest == 0) buffer.append(ch)
      }
    }
    buffer.toString
  }

  private[this] def optimizeWikipedia(term:String, qualifier:String, content:String):Option[(String, String, String)] = {
    if(qualifier == "曖昧さ回避") {
      None
    } else if(term.endsWith("一覧")) {
      None
    } else splitSentence(content).toList match {
      case List(head, _*) =>
        // 通常、文章の先頭には単語と同じ見出しが記述されているので除去
        def deleteHeading(term:String, content:String):String = {
          val normContent = normalize(content)
          if(normContent.startsWith(term) && normContent.length > term.length && Character.isWhitespace(normContent(term.length))) {
            normContent.substring(term.length).trim()
          } else normContent
        }

        val tokens = Token.parse(Wikipedia.deleteParenthesis(deleteHeading(term, head)))
        val reading = deleteDokuten(tokens).map(_.term).mkString
        Some((term, qualifier, reading))
      case Nil => None
    }
  }

  /**
    * 指定されたリード文から冗長な読点を削除します。
    *
    * @param tokens センテンス
    * @return 読点を削除したセンテンス
    */
  private[this] def deleteDokuten(tokens:Seq[Token]):Seq[Token] = {
    def noun(t:Token) = t.pos.startsWith("名詞-") || t.pos == "記号-括弧閉"

    val buf = mutable.Buffer[Token]()
    for(i <- tokens.indices) {
      // [名詞]は、[名詞]で、[名詞]には、[名詞]では、のパターンでそれぞれの読点を削除
      if(tokens(i).pos == "記号-読点" && i - 2 >= 0 && tokens(i - 1).pos == "助詞-係助詞" && (
        noun(tokens(i - 2)) ||
          (tokens(i - 2).pos.startsWith("助詞-格助詞-") && i - 3 >= 0 && noun(tokens(i - 3)))
        )) {
        /* */
      } else buf.append(tokens(i))
    }
    buf
  }
}
