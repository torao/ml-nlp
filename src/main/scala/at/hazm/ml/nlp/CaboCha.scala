package at.hazm.ml.nlp

import java.io._
import java.nio.charset.StandardCharsets
import javax.xml.parsers.DocumentBuilderFactory
import javax.xml.xpath.{XPathConstants, XPathFactory}

import at.hazm.ml.nlp.CaboCha.{Chunk, Sentence}
import org.w3c.dom.{Element, NodeList}
import org.xml.sax.InputSource

import scala.io.Source

/**
  * 係り受け解析を行うためのクラスです。外部コマンド {{cabocha}} をラップしています。
  *
  * @param cmd CaboCha の外部コマンド
  */
class CaboCha(cmd:String = "cabocha") extends AutoCloseable {
  private[this] val proc = Runtime.getRuntime.exec(Array(cmd, "-f", "3"))
  private[this] val in = new BufferedReader(new InputStreamReader(proc.getInputStream, "Windows-31j"))
  private[this] val out = new PrintWriter(new OutputStreamWriter(proc.getOutputStream, "Windows-31j"))
  private[this] val err = new BufferedReader(new InputStreamReader(proc.getErrorStream, "Windows-31j"))

  def parse(text:String):Sentence = {
    val xml = "<?xml version=\"1.0\"?>\n" + this.synchronized {
      out.println(text.replaceAll("\\r?\\n", " "))
      out.flush()
      Iterator.continually(in.readLine()).takeWhile(line => line != null && line != "</sentence>").mkString("\n")
    } + "\n</sentence>"

    val factory = DocumentBuilderFactory.newInstance()
    val builder = factory.newDocumentBuilder()
    val doc = builder.parse(new InputSource(new StringReader(xml)))

    val xpath = XPathFactory.newInstance().newXPath()
    val nl = xpath.evaluate("/sentence/chunk", doc, XPathConstants.NODESET).asInstanceOf[NodeList]
    Sentence((for(i <- 0 until nl.getLength) yield {
      val chunk = nl.item(i).asInstanceOf[Element]
      val nl2 = xpath.evaluate("tok", chunk, XPathConstants.NODESET).asInstanceOf[NodeList]
      Chunk(
        id = chunk.getAttribute("id").toInt,
        link = chunk.getAttribute("link").toInt,
        rel = chunk.getAttribute("rel"),
        score = chunk.getAttribute("score").toDouble,
        head = chunk.getAttribute("head").toInt,
        func = chunk.getAttribute("func").toInt,
        tokens = (for(j <- 0 until nl2.getLength) yield {
          val tok = nl2.item(j).asInstanceOf[Element]
          CaboCha.Token(
            id = tok.getAttribute("id").toInt,
            term = tok.getTextContent,
            feature = tok.getAttribute("feature"),
            ne = tok.getAttribute("ne"))
        }).toList)
    }).toList)
  }

  def close():Unit = synchronized {
    out.close()
    proc.waitFor()
  }

}

object CaboCha {

  case class Sentence(chunks:Seq[Chunk])

  case class Chunk(id:Int, link:Int, rel:String, score:Double, head:Int, func:Int, tokens:Seq[CaboCha.Token]){
    def headToken:Token = tokens.find{ t => t.id == head }.get
    def funcToken:Token = tokens.find{ t => t.id == func }.get
  }

  case class Token(id:Int, term:String, feature:String, ne:String)

  def simplify(text:String):Unit = {
    val cabocha = new CaboCha()
    Source.fromInputStream(System.in).getLines.map(_.trim()).filter(_.nonEmpty).foreach { line =>
      val sentence = cabocha.parse(line)
      // どこからもかかっていない chunk を取得
      val linkedChunkIds = sentence.chunks.map(_.link).toSet
      sentence.chunks.filter(c => ! linkedChunkIds.contains(c.id)).foreach{ leaf =>
        def getSequence(c:Chunk):Seq[Chunk] = {
          if(c.link < 0) Seq(c) else {
            c +: getSequence(sentence.chunks.find(_.id == c.link).get)
          }
        }
        System.out.println(getSequence(leaf).map(_.tokens.map(_.term).mkString(" ")).mkString("[", "][", "]"))
      }
    }
    cabocha.close()
  }

  def main(args:Array[String]):Unit = {
    val cabocha = new CaboCha()
    Source.fromInputStream(System.in).getLines.map(_.trim()).filter(_.nonEmpty).foreach { line =>
      val sentence = cabocha.parse(line)
      // どこからもかかっていない chunk を取得
      val linkedChunkIds = sentence.chunks.map(_.link).toSet
      sentence.chunks.filter(c => ! linkedChunkIds.contains(c.id)).foreach{ leaf =>
        def getSequence(c:Chunk):Seq[Chunk] = {
          if(c.link < 0) Seq(c) else {
            c +: getSequence(sentence.chunks.find(_.id == c.link).get)
          }
        }
        System.out.println(getSequence(leaf).map(_.tokens.map(_.term).mkString(" ")).mkString("[", "][", "]"))
      }
    }
    cabocha.close()
  }

}