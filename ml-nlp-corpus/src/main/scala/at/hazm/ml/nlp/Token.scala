package at.hazm.ml.nlp

import java.io.{BufferedReader, Reader, StringReader}

import at.hazm.ml.nlp.ja.CaboCha
import at.hazm.core.io.using

import scala.collection.mutable

/**
  * 形態素を表すクラスです。
  *
  * @param term        入力単語
  * @param pos         ハイフンでレベルを区切られた品詞
  * @param base        基本形
  * @param inflection  変化形
  * @param reading     読み
  */
case class Token(term:String, pos:String, base:Option[String], inflection:Option[String], reading:Option[String]) {
  private[this] lazy val _pos = pos.split("-").toList

  /** この形態素の品詞レベル 1 (名詞, 助詞, 動詞, 感嘆詞, etc.) を返します。 */
  def pos1:String = _pos.head

  /** この形態素の品詞レベル 2 (一般, 連体化, 係助詞, 自立, etc.) を返します。品詞レベル 2 を持たない場合は長さゼロの文字列を返します。 */
  def pos2:String = if(_pos.size >= 2) _pos(1) else ""

  /** この形態素の品詞レベル 3 を返します。品詞レベル 3 を持たない場合は長さゼロの文字列を返します。 */
  def pos3:String = if(_pos.size >= 3) _pos(2) else ""

  /** この形態素の品詞レベル 4 を返します。品詞レベル 4 を持たない場合は長さゼロの文字列を返します。 */
  def pos4:String = if(_pos.size >= 4) _pos(3) else ""
}

object Token {

  /**
    * 指定された入力単語と品詞を持つインスタンスを構築します。
    *
    * @param term 入力単語
    * @param pos  品詞
    * @return 形態素
    */
  def apply(term:String, pos:String):Token = Token(term, pos, None, None, None)

  /**
    * 指定された文字列を形態素解析して返します。文字列リテラルから生成する場合はこのメソッドの代わりに `tk"これはペンです"` のように
    * 記述することができます。
    *
    * @param text 形態素解析する文字列
    * @return 形態素解析した結果
    */
  def parse(text:String):Seq[Token] = using(new CaboCha()){ cabocha =>
    cabocha.parse(text).chunks.flatMap(_.tokens).map{ t =>
      // term:String, pos:String, base:Option[String], inflection:Option[String], reading:Option[String]
      Token(t.term, s"${t.pos1}-${t.pos2}-${t.pos3}-${t.pos4}")
    }
  }


  /**
    * 指定された入力ストリームからテキストを読み込んで形態素解析して返します。
    *
    * @param r テキストを読み込むストリーム
    * @return 形態素解析した結果
    */
  /*
  def parse(in:Reader):Seq[Token] = {
    val tk = new JapaneseTokenizer(null, false, JapaneseTokenizer.Mode.NORMAL)
    val base = tk.addAttribute(classOf[BaseFormAttribute])
    val term = tk.addAttribute(classOf[CharTermAttribute])
    val pos = tk.addAttribute(classOf[PartOfSpeechAttribute])
    val inflection = tk.addAttribute(classOf[InflectionAttribute])
    val reading = tk.addAttribute(classOf[ReadingAttribute])
    tk.setReader(in)
    tk.reset()
    val tokens = mutable.Buffer[Token]()
    while(tk.incrementToken()) {
      tokens.append(Token(term.toString, pos.getPartOfSpeech, Option(base.getBaseForm), Option(inflection.getInflectionForm), Option(reading.getReading)))
    }
    tokens
  }
  */

  /** 形態素と一致判定を行うパターン */
  private[nlp] class Pattern(term:Option[String], pos:Option[String]) {
    /** 指定された形態素と一致する場合 true */
    def matches(token:Token):Boolean = {
      term.forall(_ == token.term) && pos.forall(p => p == token.pos || token.pos.startsWith(p + "-"))
    }

    override def toString:String = s"${term.getOrElse("*")}:${pos.getOrElse("*")}"
  }

  /**
    * 形態素解析した Token のシーケンスと部分一致を判定するためのクラスです。パターンには
    * {{{"私"}}} のような入力単語 (term 部分) のみと {{{"私:名詞"}}} のような品詞付きの文字列が使用できます。入力単語と品詞はいずれも
    * ワイルドカードを示す {{{""}}} もしくは {{{"*"}}} を使用することができます。ただし {{{":"}}} や {{{"::"}}} はワイルドカードで
    * はなく記号としてのコロンと解釈されます。
    *
    * @param termsWithPOS パターンの列挙
    */
  case class Match(termsWithPOS:String*) {

    /** 一致判定用のパターン */
    private[this] val pattern:Seq[Pattern] = termsWithPOS.map { twp =>
      if(twp == ":") {
        new Pattern(Some(":"), None)
      } else if(twp.startsWith("::")) {
        val pos = twp.substring(2)
        new Pattern(Some(":"), if(pos.isEmpty || pos == "*") None else Some(pos))
      } else twp.split(":", 2) match {
        case Array(term, pos) =>
          new Pattern(if(term.isEmpty || term == "*") None else Some(term), if(pos.isEmpty || pos == "*") None else Some(pos))
        case Array(term) =>
          new Pattern(if(term.isEmpty || term == "*") None else Some(term), None)
      }
    }

    override def toString:String = pattern.map(_.toString.replaceAll("\"", "\"\"")).mkString("[\"", "\",\"", "\"]")

    /**
      * 指定された形態素シーケンス全体がこのパターンと一致しているか判定します。
      *
      * @param tokens 判定する形態素シーケンス
      * @return 全体が一致している場合 true
      */
    def matches(tokens:Seq[Token]):Boolean = tokens.length == pattern.length && indexOf(tokens) == 0

    /**
      * 指定された形態素シーケンスの前方から {{{begin}}} 以降でこのパターンと一致する部分を参照します。一致する部分が存在しない場合は
      * 負の値を返します。
      *
      * @param tokens 判定する形態素シーケンス
      * @param begin  判定を開始する位置
      * @return 部分一致している位置、または負の値
      */
    def indexOf(tokens:Seq[Token], begin:Int = 0):Int = if(tokens.length - begin < pattern.length) -1 else {
      (begin to (tokens.length - pattern.length)).find { i =>
        pattern.indices.forall(j => pattern(j).matches(tokens(i + j)))
      }.getOrElse(-1)
    }

    def contains(tokens:Seq[Token]):Boolean = indexOf(tokens) >= 0

    /**
      * 指定された形態素シーケンスの後方から {{{begin}}} 以前でこのパターンと一致する部分を参照します。一致する部分が存在しない場合は
      * 負の値を返します。
      *
      * @param tokens 判定する形態素シーケンス
      * @param begin  判定を開始する位置
      * @return 部分一致している位置、または負の値
      */
    def lastIndexOf(tokens:Seq[Token], begin:Int = -1):Int = {
      val _begin = if(begin < 0) tokens.length - 1 else begin
      if(tokens.length - (tokens.length - _begin) < pattern.length) -1 else {
        (math.min(_begin, tokens.length - pattern.length) to 0 by -1).find { i =>
          pattern.indices.forall(j => pattern(j).matches(tokens(i + j)))
        }.getOrElse(-1)
      }
    }

    /**
      * 指定された形態素シーケンスがこの Match で開始しているかを判定します。
      *
      * @param tokens 先頭一致を判定する形態素シーケンス
      * @return 先頭が一致している場合 true
      */
    def startsWith(tokens:Seq[Token]):Boolean = if(tokens.length < pattern.length) false else {
      pattern.indices.forall(i => pattern(i).matches(tokens(i)))
    }

    /**
      * 指定された形態素シーケンスがこの Match で終了しているかを判定します。
      *
      * @param tokens 末尾一致を判定する形態素シーケンス
      * @return 末尾が一致している場合 true
      */
    def endsWith(tokens:Seq[Token]):Boolean = if(tokens.length < pattern.length) false else {
      pattern.indices.forall(i => pattern(i).matches(tokens(i)))
    }

    /**
      * 指定された形態素のシーケンスのからこの Match と最初に一致する部分を {{{convert}}} が返すシーケンスに置き換えます。
      *
      * @param tokens  パターン置き換えを行うシーケンス
      * @param convert 置き換え後の形態素を決定する関数
      * @return 置き換えを行った形態素シーケンス
      */
    def replaceFirst(tokens:Seq[Token], convert:(Seq[Token]) => Seq[Token]):Seq[Token] = if(tokens.length < pattern.length) tokens else {
      val i = indexOf(tokens)
      if(i >= 0) {
        tokens.take(i) ++ convert(tokens.slice(i, i + pattern.length)) ++ tokens.drop(i + pattern.length)
      } else tokens
    }

    /**
      * 指定された形態素のシーケンスのからこの Match と一致するすべての部分を {{{convert}}} が返すシーケンスに置き換えます。
      *
      * @param tokens  パターン置き換えを行うシーケンス
      * @param convert 置き換え後の形態素を決定する関数
      * @return 置き換えを行った形態素シーケンス
      */
    def replaceAll(tokens:Seq[Token], convert:(Seq[Token]) => Seq[Token]):Seq[Token] = if(tokens.length < pattern.length) tokens else {
      if(pattern.isEmpty) {
        throw new IllegalArgumentException("replaceAll() cannot use for empty pattern")
      }
      notMatches(tokens).foldLeft(mutable.Buffer[Token]()) { case (buffer, (begin, end)) =>
        buffer.appendAll(tokens.slice(begin, end))
        if(end < tokens.length) {
          val part = tokens.slice(end, end + pattern.length)
          buffer.appendAll(convert(part))
        }
        buffer
      }
    }

    def split(tokens:Seq[Token]):Seq[Seq[Token]] = {
      if(pattern.isEmpty) {
        throw new IllegalArgumentException("split() cannot use for empty pattern")
      }
      notMatches(tokens).foldLeft(mutable.Buffer[Seq[Token]]()) { case (buffer, (begin, end)) =>
        buffer.append(tokens.slice(begin, end))
        buffer
      }
    }

    private[this] def notMatches(tokens:Seq[Token], fromIndex:Int = 0):Stream[(Int, Int)] = {
      val i = indexOf(tokens, fromIndex)
      if(i < 0) {
        (fromIndex, tokens.length) #:: Stream.empty[(Int,Int)]
      } else {
        (fromIndex, i) #:: notMatches(tokens, i + pattern.length)
      }
    }
  }

}