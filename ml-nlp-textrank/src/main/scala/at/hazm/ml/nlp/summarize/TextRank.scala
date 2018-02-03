/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements; and to You under the Apache License, Version 2.0.
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package at.hazm.ml.nlp.summarize

import java.io.StringReader

import edu.uci.ics.jung.algorithms.scoring.PageRank
import edu.uci.ics.jung.graph.UndirectedOrderedSparseMultigraph
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.codelibs.neologd.ipadic.lucene.analysis.ja.JapaneseTokenizer
import org.codelibs.neologd.ipadic.lucene.analysis.ja.tokenattributes.{BaseFormAttribute, InflectionAttribute, PartOfSpeechAttribute, ReadingAttribute}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.mutable

object TextRank {
  private[this] val logger = LoggerFactory.getLogger(getClass.getName.dropRight(1))

  def extract(doc:String, words:Int = 5, windowSize:Int = 2, splitSentenceWith:String = "", selector:(Morph) => Boolean = defaultFilter):Seq[(String, Double)] = {
    val sentences = (if(splitSentenceWith.nonEmpty) doc.split(splitSentenceWith).filter(_.nonEmpty).toSeq else Seq(doc)).map { sentence =>
      tokenize(sentence).filter(selector).map { m =>
        s"${if(m.baseForm.nonEmpty) m.baseForm else m.surface}:${m.pos1}-${m.pos2}-${m.pos3}-${m.pos4}"
      }.toIndexedSeq
    }
    logger.trace(sentences.flatten.mkString("tokenize: [", ", ", "]"))
    System.err.println(sentences.flatten.mkString("tokenize: [", ", ", "]"))
    val graph = new UndirectedOrderedSparseMultigraph[String, String]()
    sentences.flatMap { tokens =>
      for(
        (i, t1) <- tokens.indices.dropRight(1).map(i => (i, tokens(i)));
        t2 <- (1 until math.min(windowSize, tokens.length - i)).map(j => tokens(i + j))
      ) yield (s"$t1,$t2", (t1, t2))
    }.groupBy(_._1).mapValues(_.head._2).foreach { case (key, (t1, t2)) =>
      graph.addVertex(t1)
      graph.addVertex(t2)
      graph.addEdge(key, t1, t2)
    }
    val pageRank = new PageRank[String, String](graph, 0.85)
    pageRank.setTolerance(0.0001)
    pageRank.evaluate()
    graph.getVertices.asScala.map { token =>
      (token.split(":").head, pageRank.getVertexScore(token).toDouble)
    }.toSeq.sortBy(-_._2).take(words)
  }

  private[this] def defaultFilter(morph:Morph):Boolean = morph.pos1 == "名詞" || morph.pos1 == "形容詞"

  /**
    * 指定された文字列を Kuromoji + IPADIC + NEologd を用いて形態素解析します。
    *
    * @param text 形態素解析する文字列
    * @return 処理結果
    */
  private[this] def tokenize(text:String):Seq[Morph]

  = {

    val tokenizer = new JapaneseTokenizer(null, false, JapaneseTokenizer.Mode.EXTENDED)
    val term = tokenizer.getAttribute(classOf[CharTermAttribute])
    val pos = tokenizer.getAttribute(classOf[PartOfSpeechAttribute])
    val baseForm = tokenizer.getAttribute(classOf[BaseFormAttribute])
    val inflection = tokenizer.getAttribute(classOf[InflectionAttribute])
    val reading = tokenizer.getAttribute(classOf[ReadingAttribute])

    tokenizer.setReader(new StringReader(text))
    tokenizer.reset()

    // 形態素に分解
    val buffer = mutable.Buffer[Morph]()
    while(tokenizer.incrementToken()) {
      val Array(pos1, pos2, pos3, pos4) = pos.getPartOfSpeech.split("-").padTo(4, "")
      val morph = Morph(
        surface = term.toString,
        pos1 = pos1, pos2 = pos2, pos3 = pos3, pos4 = pos4,
        baseForm = Option(baseForm.getBaseForm).getOrElse(""),
        conjugationForm = Option(inflection.getInflectionForm).getOrElse(""),
        conjugationType = Option(inflection.getInflectionType).getOrElse(""),
        reading = Option(reading.getReading).getOrElse(""),
        pronunciation = Option(reading.getPronunciation).getOrElse(""))
      buffer.append(morph)
    }
    tokenizer.close()

    buffer.toList
  }

  case class Morph(surface:String, pos1:String, pos2:String, pos3:String, pos4:String,
                   baseForm:String, conjugationForm:String, conjugationType:String,
                   reading:String, pronunciation:String) {
  }

  case class Relation(morph1:Morph, morph2:Morph)

  def main(args:Array[String]):Unit = {
    import java.io._
    val in = new BufferedReader(new InputStreamReader(System.in))
    Iterator.continually(in.readLine()).takeWhile(_ != null).foreach { text =>
      extract(text, splitSentenceWith = "。").foreach { case (token, score) =>
        System.out.println(f"$score%.6f: $token")
      }
    }
  }
}