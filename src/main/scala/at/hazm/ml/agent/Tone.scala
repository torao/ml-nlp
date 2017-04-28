package at.hazm.ml.agent

import at.hazm.ml.nlp.Token

trait Tone {
  def talk(speech:Seq[Token]):Seq[Token]
}

object Tone {

  object NONE extends Tone {
    def talk(speech:Seq[Token]):Seq[Token] = speech
  }

  object DESU_MASU extends Tone {
    private[this] val DE_ARU = Token.Match("で:助動詞", "ある:助動詞", "。:記号-句点")
    private[this] val TAIGEN_DOME = Token.Match("*:名詞", "。:記号-句点")
    private[this] val DOSHI_DOME = Token.Match("*:動詞", "。:記号-句点")
    private[this] val DESU = Token("です", "助動詞", None, Some("基本形"), Some("デス"))
    private[this] val MASU = Token("ます", "助動詞", None, Some("基本形"), Some("マス"))
    private[this] val KUTEN = Token("。", "記号-句点", None, None, Some("。"))
    private[this] val DOSHI_RENYO = Map(
      "指す" -> ("指し", "サシ"),
      "示す" -> ("示し", "シメシ"),
      "表す" -> ("表し", "アラワシ"),
      "言う" -> ("言い", "イイ"),
      "いう" -> ("言い", "イイ"),
      "いる" -> ("い", "イ")
    )
    override def talk(speech:Seq[Token]):Seq[Token] = {
      val t1 = DE_ARU.replaceAll(speech, { _ => Seq(DESU, KUTEN) })
      val t2 = TAIGEN_DOME.replaceAll(t1, { t => t.dropRight(1) :+ DESU :+ KUTEN})
      DOSHI_DOME.replaceAll(t2, { t =>
        DOSHI_RENYO.get(t.head.term) match {
          case Some((renyo, yomi)) =>
            Seq(Token(renyo, "動詞-自立", Some(t.head.term), Some("連用形"), Some(yomi)), MASU, KUTEN)
          case None => t
        }
      })
    }
  }

}