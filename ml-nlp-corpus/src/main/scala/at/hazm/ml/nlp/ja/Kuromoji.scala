package at.hazm.ml.nlp.ja

import java.io.StringReader

import at.hazm.ml.nlp.model.Morph
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.codelibs.neologd.ipadic.lucene.analysis.ja.JapaneseTokenizer
import org.codelibs.neologd.ipadic.lucene.analysis.ja.tokenattributes.{BaseFormAttribute, InflectionAttribute, PartOfSpeechAttribute, ReadingAttribute}

import scala.collection.mutable

/**
  * 日本語の形態素解析 Kuromoji + NEologd を使用した Pipe。
  */
object Kuromoji {

  /**
    * 指定された文字列を Kuromoji + IPADIC + NEologd を用いて形態素解析します。
    *
    * @param text 形態素解析する文字列
    * @return 処理結果
    * @see [[at.hazm.ml.nlp.ja.normalize()]]
    * @see [[at.hazm.ml.nlp.ja.combineNegatives()]]
    */
  def tokenize(text:String):Seq[(Morph, Morph.Instance)] = {

    val tokenizer = new JapaneseTokenizer(null, false, JapaneseTokenizer.Mode.NORMAL)
    val term = tokenizer.getAttribute(classOf[CharTermAttribute])
    val pos = tokenizer.getAttribute(classOf[PartOfSpeechAttribute])
    val baseForm = tokenizer.getAttribute(classOf[BaseFormAttribute])
    val inflection = tokenizer.getAttribute(classOf[InflectionAttribute])
    val reading = tokenizer.getAttribute(classOf[ReadingAttribute])

    tokenizer.setReader(new StringReader(text))
    tokenizer.reset()

    // 形態素に分解
    val buffer = mutable.Buffer[(Morph, Morph.Instance)]()
    while(tokenizer.incrementToken()) {
      val Array(pos1, pos2, pos3, pos4) = pos.getPartOfSpeech.split("-").padTo(4, "")
      val morph = Morph(
        surface = Option(baseForm.getBaseForm).getOrElse(term.toString),
        pos1 = pos1, pos2 = pos2, pos3 = pos3, pos4 = pos4)
      val instance = Morph.Instance(-1, surface = term.toString,
        conjugationForm = Option(inflection.getInflectionForm).getOrElse(""),
        conjugationType = Option(inflection.getInflectionType).getOrElse(""),
        reading = Option(reading.getReading).getOrElse(""),
        pronunciation = Option(reading.getPronunciation).getOrElse(""), attr = Map.empty)
      buffer.append((morph, instance))
    }
    tokenizer.close()

    buffer.toList
  }
}