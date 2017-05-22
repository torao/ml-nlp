package at.hazm.ml.nlp

import play.api.libs.json.{JsObject, Json}

import scala.collection.mutable

/**
  * 形態素を現すクラスです。
  * IPADIC をベースとした形態素解析と同等のプロパティを持ちます。
  *
  * @param surface         表現
  * @param pos1            品詞レベル 1 (名詞, 助詞, 動詞, 感嘆詞, etc.)
  * @param pos2            品詞レベル 2 (一般, 連体化, 係助詞, 自立, etc.)
  * @param pos3            品詞レベル 3 (組織, 一般, etc.)
  * @param pos4            品詞レベル 4
  * @param conjugationType 活用型
  * @param conjugationForm 活用形
  * @param baseForm        基本形
  * @param reading         読み
  * @param pronunciation   発音
  * @see https://github.com/atilika/kuromoji/blob/master/kuromoji-ipadic/src/main/java/com/atilika/kuromoji/ipadic/Token.java
  */
case class Morph(surface:String, pos1:String, pos2:String, pos3:String, pos4:String,
                 conjugationType:String, conjugationForm:String, baseForm:String, reading:String, pronunciation:String) {

  /**
    * この形態素の品詞をハイフンで連結した文字列です。
    */
  lazy val pos:String = Seq(pos1, pos2, pos3, pos4).reverse.dropWhile(_.isEmpty).reverse.mkString("-")

  /**
    * 形態素の同一性を比較するためのキーを参照します。このキーが一致する形態素は同一とみなすことができます。
    *
    * @return 同一性のキー
    */
  def key:String = surface + ":" + pos

  /**
    * この形態素の内容を JSON 形式に変換します。
    *
    * @return JSON 表現
    */
  def toJSON:JsObject = Json.obj(
    "surface" -> surface,
    "pos" -> Json.arr(pos1, pos2, pos3, pos4),
    "conj-type" -> conjugationType,
    "conj-form" -> conjugationForm,
    "base" -> baseForm,
    "reading" -> reading,
    "pronunciation" -> pronunciation
  )
}

object Morph {

  /**
    * 指定された JSON 表現から形態素情報を復元します。
    *
    * @param json 形態素を復元する JSON
    * @return 復元した形態素
    */
  def fromJSON(json:JsObject):Morph = Morph(
    surface = (json \ "surface").as[String],
    pos1 = (json \ "pos").apply(0).as[String],
    pos2 = (json \ "pos").apply(1).as[String],
    pos3 = (json \ "pos").apply(2).as[String],
    pos4 = (json \ "pos").apply(3).as[String],
    conjugationType = (json \ "conj-type").as[String],
    conjugationForm = (json \ "conj-form").as[String],
    baseForm = (json \ "base").as[String],
    reading = (json \ "reading").as[String],
    pronunciation = (json \ "pronunciation").as[String]
  )

  /**
    * 形態素と一致判定を行うパターン。
    */
  private[nlp] class Pattern(term:Option[String], pos:Option[String]) {
    /** 指定された形態素と一致する場合 true */
    def matches(token:Morph):Boolean = {
      term.forall(_ == token.surface) && pos.forall(p => p == token.pos1 || token.pos1.startsWith(p + "-"))
    }

    override def toString:String = s"${term.getOrElse("*")}:${pos.getOrElse("*")}"
  }

  /**
    * 形態素解析した Morph のシーケンスと部分一致を判定するためのクラスです。パターンには `"私"` のような入力単語 (surface 部分のみ)
    * または `"私:名詞"` のような品詞付きの文字列が使用できます。入力単語と品詞はいずれもワイルドカードを示す `"私:"` もしくは `"私:*"`
    * を使用することができます。ただし `":"` や `"::"` はワイルドカードではなく記号としてのコロンと解釈されます。
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
    def matches(tokens:Seq[Morph]):Boolean = tokens.length == pattern.length && indexOf(tokens) == 0

    /**
      * 指定された形態素シーケンスの前方から {{{begin}}} 以降でこのパターンと一致する部分を参照します。一致する部分が存在しない場合は
      * 負の値を返します。
      *
      * @param tokens 判定する形態素シーケンス
      * @param begin  判定を開始する位置
      * @return 部分一致している位置、または負の値
      */
    def indexOf(tokens:Seq[Morph], begin:Int = 0):Int = if(tokens.length - begin < pattern.length) -1 else {
      (begin to (tokens.length - pattern.length)).find { i =>
        pattern.indices.forall(j => pattern(j).matches(tokens(i + j)))
      }.getOrElse(-1)
    }

    def contains(tokens:Seq[Morph]):Boolean = indexOf(tokens) >= 0

    /**
      * 指定された形態素シーケンスの後方から {{{begin}}} 以前でこのパターンと一致する部分を参照します。一致する部分が存在しない場合は
      * 負の値を返します。
      *
      * @param tokens 判定する形態素シーケンス
      * @param begin  判定を開始する位置
      * @return 部分一致している位置、または負の値
      */
    def lastIndexOf(tokens:Seq[Morph], begin:Int = -1):Int = {
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
    def startsWith(tokens:Seq[Morph]):Boolean = if(tokens.length < pattern.length) false else {
      pattern.indices.forall(i => pattern(i).matches(tokens(i)))
    }

    /**
      * 指定された形態素シーケンスがこの Match で終了しているかを判定します。
      *
      * @param tokens 末尾一致を判定する形態素シーケンス
      * @return 末尾が一致している場合 true
      */
    def endsWith(tokens:Seq[Morph]):Boolean = if(tokens.length < pattern.length) false else {
      pattern.indices.forall(i => pattern(i).matches(tokens(i)))
    }

    /**
      * 指定された形態素のシーケンスのからこの Match と最初に一致する部分を {{{convert}}} が返すシーケンスに置き換えます。
      *
      * @param tokens  パターン置き換えを行うシーケンス
      * @param convert 置き換え後の形態素を決定する関数
      * @return 置き換えを行った形態素シーケンス
      */
    def replaceFirst(tokens:Seq[Morph], convert:(Seq[Morph]) => Seq[Morph]):Seq[Morph] = if(tokens.length < pattern.length) tokens else {
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
    def replaceAll(tokens:Seq[Morph], convert:(Seq[Morph]) => Seq[Morph]):Seq[Morph] = if(tokens.length < pattern.length) tokens else {
      if(pattern.isEmpty) {
        throw new IllegalArgumentException("replaceAll() cannot use for empty pattern")
      }
      notMatches(tokens).foldLeft(mutable.Buffer[Morph]()) { case (buffer, (begin, end)) =>
        buffer.appendAll(tokens.slice(begin, end))
        if(end < tokens.length) {
          val part = tokens.slice(end, end + pattern.length)
          buffer.appendAll(convert(part))
        }
        buffer
      }
    }

    def split(tokens:Seq[Morph]):Seq[Seq[Morph]] = {
      if(pattern.isEmpty) {
        throw new IllegalArgumentException("split() cannot use for empty pattern")
      }
      notMatches(tokens).foldLeft(mutable.Buffer[Seq[Morph]]()) { case (buffer, (begin, end)) =>
        buffer.append(tokens.slice(begin, end))
        buffer
      }
    }

    private[this] def notMatches(tokens:Seq[Morph], fromIndex:Int = 0):Stream[(Int, Int)] = {
      val i = indexOf(tokens, fromIndex)
      if(i < 0) {
        (fromIndex, tokens.length) #:: Stream.empty[(Int, Int)]
      } else {
        (fromIndex, i) #:: notMatches(tokens, i + pattern.length)
      }
    }
  }

}