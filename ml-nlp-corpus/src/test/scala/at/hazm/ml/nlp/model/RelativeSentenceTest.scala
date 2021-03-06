/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements; and to You under the Apache License, Version 2.0.
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package at.hazm.ml.nlp.model

import org.specs2.Specification
import org.specs2.execute.Result

class RelativeSentenceTest extends Specification {
  override def is =
    s2"""
文節の係り受け関係を認識して最短文を作成する: $e0
"""

  def e0:Result = {
    val results:Seq[Result] = RelativeSentence(Seq(
      RelativeClause(0, 1, "D", 1.0, 0, 0, Seq(Morph.Instance(0, "A", "", "", "", "", Map.empty))),
      RelativeClause(1, 4, "D", 1.0, 0, 0, Seq(Morph.Instance(1, "B", "", "", "", "", Map.empty))),
      RelativeClause(2, 3, "D", 1.0, 0, 0, Seq(Morph.Instance(2, "C", "", "", "", "", Map.empty))),
      RelativeClause(3, 4, "D", 1.0, 0, 0, Seq(Morph.Instance(3, "D", "", "", "", "", Map.empty))),
      RelativeClause(4, -1, "D", 1.0, 0, 0, Seq(Morph.Instance(4, "E", "", "", "", "", Map.empty)))
    )).breakdown().map { sentence =>
      sentence.tokens.map(_.morphId).toList match {
        case 0 :: 1 :: 4 :: Nil => success
        case 2 :: 3 :: 4 :: Nil => success
        case _ => failure
      }
    }
    results.reduceLeft { (a, b) => a and b }
  }

}
