package at.hazm.ml.nlp

import java.io.{Reader, StringReader}

import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.codelibs.neologd.ipadic.lucene.analysis.ja.JapaneseTokenizer
import org.codelibs.neologd.ipadic.lucene.analysis.ja.tokenattributes.{BaseFormAttribute, InflectionAttribute, PartOfSpeechAttribute, ReadingAttribute}

import scala.collection.mutable

/**
  * 形態素を表すクラス
  *
  * @param term       入力単語
  * @param pos        品詞
  * @param base       基本形
  * @param inflection 変化形
  * @param reading    読み
  */
case class Token(term:String, pos:String, base:Option[String], inflection:Option[String], reading:Option[String]) {
  /**
    * この形態素が文の終末をあらわすかを返します。
    *
    * @return 文の終末の場合 true
    */
  def isPeriod:Boolean = pos == "記号-句点"
}

object Token {

  /**
    * 指定された文字列を形態素解析して返します。
    *
    * @param text 形態素解析する文字列
    * @return 形態素解析した結果
    */
  def parse(text:String):Seq[Token] = parse(new StringReader(text))

  /**
    * 指定された入力ストリームからテキストを読み込んで形態素解析して返します。
    *
    * @param in テキストを読み込むストリーム
    * @return 形態素解析した結果
    */
  def parse(in:Reader):Seq[Token] = {
    // val dic = new UserDictionary()
    val tk = new JapaneseTokenizer(null, false, JapaneseTokenizer.Mode.NORMAL)
    val base = tk.addAttribute(classOf[BaseFormAttribute])
    val term = tk.addAttribute(classOf[CharTermAttribute])
    val pos = tk.addAttribute(classOf[PartOfSpeechAttribute])
    val inflection = tk.addAttribute(classOf[InflectionAttribute])
    val reading = tk.addAttribute(classOf[ReadingAttribute])
    tk.setReader(in)
    tk.reset()
    val tokens = mutable.Buffer[Token]()
    while (tk.incrementToken()) {
      tokens.append(Token(term.toString, pos.getPartOfSpeech, Option(base.getBaseForm), Option(inflection.getInflectionForm), Option(reading.getReading)))
    }
    tokens
  }

  def main(args:Array[String]):Unit = {
    def _parse(in:java.io.InputStream):Unit = scala.io.Source.fromInputStream(System.in).getLines().foreach { line =>
      parse(line).zipWithIndex.foreach { case (token, i) =>
        System.out.println(f"${i + 1}%02d: $token")
      }
    }

    if (args.isEmpty) _parse(System.in) else args.foreach { file =>
      val in = new java.io.FileInputStream(file)
      _parse(in)
      in.close()
    }
  }

}