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

  def tokenizeAndScore(doc:String, windowSize:Int = 2, splitSentenceWith:String = "", selector:(Morph) => Boolean = defaultFilter, alpha:Double = 0.85, tolerance:Double = 0.0001):Seq[(String, Double)] = {
    val sentences = (if(splitSentenceWith.nonEmpty) doc.split(splitSentenceWith).filter(_.nonEmpty).toSeq else Seq(doc)).map { sentence =>
      tokenize(sentence).filter(selector).map { m =>
        s"${if(m.baseForm.nonEmpty) m.baseForm else m.surface}:${m.pos1}-${m.pos2}-${m.pos3}-${m.pos4}"
      }.toIndexedSeq
    }
    score(sentences, windowSize = windowSize, alpha = alpha, tolerance = tolerance)
  }

  /**
    * 指定されたパラグラフ (文の集合) または文書集合からテキスト要素の重要度をスコアリングします。
    *
    * @param sentences  テキスト要素のシーケンス (パラグラフまたは文書集合)
    * @param windowSize 関連があるとみなす隣接要素のウィンドウサイズ
    * @param alpha      PageRank の減衰係数
    * @param tolerance  PageRank の収束しきい値
    * @tparam T テキスト要素の型
    * @return (テキスト要素, スコア) の昇順ソート済み列挙
    */
  def score[T](sentences:Seq[Seq[T]], windowSize:Int = 2, alpha:Double = 0.85, tolerance:Double = 0.0001):Seq[(T, Double)] = {

    // ウィンドウサイズ内で隣接したテキスト要素から無向重み付きグラフを作成
    val map = sentences.flatMap { sentence =>
      for(
        (i, t1) <- sentence.indices.dropRight(1).map(i => (i, sentence(i)));
        t2 <- (1 until math.min(windowSize, sentence.length - i)).map(j => sentence(i + j))
      ) yield (s"$t1\t$t2", (t1, t2))
    }.groupBy(_._1).mapValues { x => (x.head._2, x.length) }

    val graph = map.foldLeft(new UndirectedOrderedSparseMultigraph[T, String]()) { case (g, (key, ((t1, t2), _))) =>
      g.addVertex(t1)
      g.addVertex(t2)
      g.addEdge(key, t1, t2)
      g
    }

    // ページランクを実行してスコアとともに返す
    val pageRank = new PageRank[T, String](graph, { key => map(key)._2 }, alpha)
    pageRank.setTolerance(tolerance)
    pageRank.evaluate()
    graph.getVertices.asScala.map { token =>
      (token, pageRank.getVertexScore(token).toDouble)
    }.toSeq.sortBy(-_._2)
  }

  /**
    * 名詞と形容詞のみを対象とする構文フィルター。
    *
    * @param morph 判定する形態素
    * @return 名詞か形容詞の場合 true
    */
  private[this] def defaultFilter(morph:Morph):Boolean = morph.pos1 == "名詞" || morph.pos1 == "形容詞"

  /**
    * 指定された文字列を Kuromoji + IPADIC + NEologd を用いて形態素解析します。
    *
    * @param text 形態素解析する文字列
    * @return 処理結果
    */
  private[this] def tokenize(text:String):Seq[Morph] = {

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

  def main(args:Array[String]):Unit = {
    import java.io._
    val in = new BufferedReader(new InputStreamReader(System.in))
    Iterator.continually(in.readLine()).takeWhile(_ != null).foreach { text =>
      tokenizeAndScore(text, splitSentenceWith = "。").foreach { case (token, score) =>
        System.out.println(f"$score%.6f: $token")
      }
    }
  }
}