package at.hazm.ml.nlp

import at.hazm.ml.nlp.Paragraph.{Clause, MorphIndex, Sentence}
import org.specs2.Specification
import org.specs2.execute.Result

class ParagraphTest extends Specification {
  override def is =
    s2"""
文節の係り受け関係を認識して最短文を作成する: $e0
"""

  def e0:Result = {
    val results:Seq[Result] = Sentence(0, Seq(
      Clause(0, 1, "D", 1.0, 0, 0, Seq(MorphIndex(0, 0, ""))),
      Clause(1, 4, "D", 1.0, 0, 0, Seq(MorphIndex(1, 1, ""))),
      Clause(2, 3, "D", 1.0, 0, 0, Seq(MorphIndex(2, 2, ""))),
      Clause(3, 4, "D", 1.0, 0, 0, Seq(MorphIndex(3, 3, ""))),
      Clause(4, -1, "D", 1.0, 0, 0, Seq(MorphIndex(4, 4, "")))
    )).breakdown().map { sentence =>
      sentence.toIndices.toList match {
        case 0 :: 1 :: 4 :: Nil => success
        case 2 :: 3 :: 4 :: Nil => success
        case _ => failure
      }
    }
    results.reduceLeft { (a, b) => a and b }
  }

}
