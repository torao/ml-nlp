package at.hazm.ml.nlp.ja

import java.io._
import javax.xml.parsers.DocumentBuilderFactory

import at.hazm.core.XML._
import at.hazm.ml.nlp.{Corpus, Morph, Paragraph}
import org.xml.sax.InputSource

/**
  * 係り受け解析を行うためのクラスです。外部コマンド {{cabocha}} をラップしています。
  *
  * @param cmd CaboCha の外部コマンド
  */
class CaboCha(cmd:String = "cabocha") extends AutoCloseable {
  private[this] val proc = Runtime.getRuntime.exec(Array(cmd, "-f", "3", "-n", "1"))
  private[this] val in = new BufferedReader(new InputStreamReader(proc.getInputStream, "Windows-31j"))
  private[this] val out = new PrintWriter(new OutputStreamWriter(proc.getOutputStream, "Windows-31j"))
  private[this] val err = new BufferedReader(new InputStreamReader(proc.getErrorStream, "Windows-31j"))

  def parse(id:Int, text:String, corpus:Corpus):Paragraph = {
    val paragraph = Paragraph(id, at.hazm.ml.nlp.ja.splitSentence(text).zipWithIndex.map { case (sentence, i) =>
      parseSentence(i, sentence, corpus)
    })
    corpus.paragraphs.set(id, paragraph)
    paragraph
  }

  private[this] def parseSentence(sentenceId:Int, sentence:String, corpus:Corpus):Paragraph.Sentence = {
    val xml = "<?xml version=\"1.0\"?>\n" + this.synchronized {
      out.println(sentence.replaceAll("\\r?\\n", " "))
      out.flush()
      Iterator.continually(in.readLine()).takeWhile(line => line != null && line != "</sentence>").mkString("\n")
    } + "\n</sentence>"

    val factory = DocumentBuilderFactory.newInstance()
    val builder = factory.newDocumentBuilder()
    val doc = builder.parse(new InputSource(new StringReader(xml)))

    val root = doc.getDocumentElement
    val clauses = if(root.getTagName == "sentence") {
      root.getChildNodes.toElements.filter(_.getTagName == "chunk").map { chunk =>
        val toks = chunk.getChildNodes.toElements.filter(_.getTagName == "tok")
        val tokens = toks.map { tok =>
          CaboCha.Token(
            id = tok.getAttribute("id").toInt,
            term = tok.getTextContent,
            feature = tok.getAttribute("feature"),
            ne = tok.getAttribute("ne"))
        }

        // 文書内の形態素を登録して Key -> Index のマップを作製
        val morphs = tokens.map(_.toMorph)
        val idMap = corpus.vocabulary.registerAll(morphs).zip(morphs).map { case (i, m) => (m.key, i) }.toMap

        // この文節の形態素インデックス
        val morphIndices = tokens.map { token =>
          Paragraph.MorphIndex(
            id = token.id,
            morphId = idMap(token.toMorph.key),
            ne = token.ne
          )
        }

        Paragraph.Clause(
          id = chunk.getAttribute("id").toInt,
          link = chunk.getAttribute("link").toInt,
          rel = chunk.getAttribute("rel"),
          score = chunk.getAttribute("score").toDouble,
          head = chunk.getAttribute("head").toInt,
          func = chunk.getAttribute("func").toInt,
          morphs = morphIndices
        )
      }
    } else Seq.empty
    Paragraph.Sentence(sentenceId, clauses)
  }

  def close():Unit = synchronized {
    out.close()
    proc.waitFor()
  }

}

object CaboCha {

  private[CaboCha] case class Token(id:Int, term:String, feature:String, ne:String) {
    lazy val (pos1, pos2, pos3, pos4, conjugationType, conjugationForm, baseForm, reading, pronunciation) = feature.split(",") match {
      case Array(p1, p2, p3, p4, ct, cf, bf, r, p) =>
        (p1, p2, p3, p4, ct, cf, bf, r, p)
      case Array(p1, p2, p3, p4, ct, cf, bf) =>
        (p1, p2, p3, p4, ct, cf, bf, "*", "*")
    }

    def toMorph:Morph = {
      def a(text:String):String = if(text == "*") "" else text

      Morph(
        surface = term, pos1 = a(pos1), pos2 = a(pos2), pos3 = a(pos3), pos4 = a(pos4),
        conjugationType = a(conjugationType), conjugationForm = a(conjugationForm),
        baseForm = a(baseForm), reading = a(reading), pronunciation = a(pronunciation)
      )
    }

  }

}