package at.hazm.ml.nlp.ja

import java.io._
import javax.xml.parsers.DocumentBuilderFactory

import at.hazm.core.XML._
import at.hazm.ml.nlp.ja.CaboCha.Token
import at.hazm.ml.nlp.model.Morph.AbstractMorph
import at.hazm.ml.nlp.model.{Morph, RelativeClause, RelativeDocument, RelativeSentence}
import at.hazm.ml.nlp.tokenizer.RelativeTokenizer
import org.xml.sax.InputSource
import play.api.libs.json.{JsValue, Json}

/**
  * 係り受け解析を行うためのクラスです。外部コマンド {{cabocha}} をラップしています。
  *
  * @param cmd CaboCha の外部コマンド
  */
class CaboCha(cmd:String = "cabocha") extends AutoCloseable with RelativeTokenizer[Token, RelativeSentence[Token]] {
  private[this] val proc = Runtime.getRuntime.exec(Array(cmd, "-f", "3", "-n", "1"))
  private[this] val in = new BufferedReader(new InputStreamReader(proc.getInputStream, "Windows-31j"))
  private[this] val out = new PrintWriter(new OutputStreamWriter(proc.getOutputStream, "Windows-31j"))
  private[this] val err = new BufferedReader(new InputStreamReader(proc.getErrorStream, "Windows-31j"))

  private[this] val factory = DocumentBuilderFactory.newInstance()

  /**
    * 指定された文章を係り受け解析しトークンに分割します。
    *
    * @param id        文章のID
    * @param paragraph 係り受け解析する文章
    * @return 解析された文章
    */
  override def tokenize(id:Int, paragraph:String):RelativeDocument[Token] = {
    val sentences = splitSentence(paragraph).map { sentence =>

      // 形態素解析 & 係り受け解析の実行
      val xml = "<?xml version=\"1.0\"?>\n" + this.synchronized {
        out.println(sentence.replaceAll("\\r?\\n", " "))
        out.flush()
        Iterator.continually(in.readLine()).takeWhile(line => line != null && line != "</sentence>").mkString("\n")
      } + "\n</sentence>"

      // XML の解析
      val builder = factory.newDocumentBuilder()
      val doc = builder.parse(new InputSource(new StringReader(xml)))

      val root = doc.getDocumentElement
      val clauses = if(root.getTagName == "sentence") {
        root.getChildNodes.toElements.filter(_.getTagName == "chunk").map { chunk =>

          // 文節を構成する形態素を取得
          val toks = chunk.getChildNodes.toElements.filter(_.getTagName == "tok")
          val tokens = toks.map { tok =>
            CaboCha.Token(
              id = tok.getAttribute("id").toInt,
              term = tok.getTextContent,
              feature = tok.getAttribute("feature"),
              ne = tok.getAttribute("ne"))
          }

          // 形態素をまとめて文節を作成
          RelativeClause(
            id = chunk.getAttribute("id").toInt,
            link = chunk.getAttribute("link").toInt,
            rel = chunk.getAttribute("rel"),
            score = chunk.getAttribute("score").toDouble,
            head = chunk.getAttribute("head").toInt,
            func = chunk.getAttribute("func").toInt,
            tokens = tokens
          )
        }
      } else Seq.empty
      RelativeSentence(clauses)
    }
    RelativeDocument(id, sentences)
  }

  def close():Unit = synchronized {
    out.close()
    proc.waitFor()
  }

}

object CaboCha {

  /**
    * CaboCha の係り受け解析によって生成されるトークンです。
    *
    * @param id      トークンの ID
    * @param term    単語
    * @param feature 特徴
    * @param ne      関係
    */
  case class Token(id:Int, term:String, feature:String, ne:String) extends AbstractMorph {
    lazy val surface:String = term
    lazy val (pos1, pos2, pos3, pos4, conjugationType, conjugationForm, baseForm, reading, pronunciation) = feature.split(",") match {
      case Array(p1, p2, p3, p4, ct, cf, bf, r, p) =>
        (p1, p2, p3, p4, ct, cf, bf, r, p)
      case Array(p1, p2, p3, p4, ct, cf, bf) =>
        (p1, p2, p3, p4, ct, cf, bf, "*", "*")
    }

    /**
      * 形態素の同一性を比較するためのキーを参照します。このキーが一致する形態素は同一とみなすことができます。
      *
      * @return 同一性のキー
      */
    def key:String = s"$baseForm:$pos"

    /**
      * CaboCha によって解析された形態素を標準形式の形態素に変換します。
      */
    def toMorph:Morph = Morph(if(baseForm != null && baseForm != "" && baseForm != "*") baseForm else surface, pos1, pos2, pos3, pos4)

    def toInstance(morphId:Int):Morph.Instance = {
      val attr:Map[String, String] = if(ne == "*" || ne.isEmpty) Map.empty else Map("ne" -> ne)
      Morph.Instance(morphId, surface, conjugationType, conjugationForm, reading, pronunciation, attr)
    }

    def toJSON:JsValue = Json.obj("id" -> id, "term" -> term, "feature" -> feature, "ne" -> ne)
  }

}