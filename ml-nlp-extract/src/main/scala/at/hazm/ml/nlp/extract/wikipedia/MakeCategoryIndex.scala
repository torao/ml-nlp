package at.hazm.ml.nlp.extract.wikipedia

import java.io._
import java.nio.charset.StandardCharsets
import javax.xml.parsers.SAXParserFactory

import at.hazm.core.io.using
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream
import org.xml.sax.{Attributes, Locator}
import org.xml.sax.helpers.DefaultHandler

import scala.collection.mutable

object MakeCategoryIndex {
  def main(args:Array[String]):Unit = {
    if(args.length != 2) {
      help("Please specify wikipedia dump file compressed by bzip2")
    }
    val src = new File(args.head)
    if(!src.isFile) {
      help(s"specified file is not exist or not file: $src")
    }

    val dst = new File(args(1))
    using(new PrintWriter(new OutputStreamWriter(new FileOutputStream(dst), StandardCharsets.UTF_8))) { out =>
      using(new BZip2CompressorInputStream(new FileInputStream(src))) { in =>
        val factory = SAXParserFactory.newInstance()
        val parser = factory.newSAXParser()
        parser.parse(in, new Parser(out))
      }
    }

  }

  class Parser(out:PrintWriter) extends DefaultHandler {
    private[this] val buffer = new StringBuilder()

    private[this] val stack = mutable.Stack[String]()
    private[this] var id = -1
    private[this] var title = ""
    private[this] var text = ""

    override def characters(ch:Array[Char], start:Int, length:Int):Unit = {
      buffer.appendAll(ch, start, length)
    }

    override def setDocumentLocator(locator:Locator):Unit = {
      out.println("PAGE-ID\tTITLE\tCATEGORIES*")
    }

    override def startElement(uri:String, localName:String, qName:String, attributes:Attributes):Unit = {
      stack.push(qName)
    }

    override def endElement(uri:String, localName:String, qName:String):Unit = {
      if(stack.pop() != qName){
        throw new IllegalStateException(s"invalid end position: $qName")
      }
      if(stack.head == "page"){
        qName match {
          case "id" => this.id = buffer.toString().trim().toInt
          case "title" => this.title = buffer.toString().trim()
          case _ => None
        }
      } else qName match {
        case "text" =>
          this.text = buffer.toString().trim()
        case "page" =>
          val cats = extractCategories(text).distinct.sorted.mkString("\t").replaceAll("\\s+", " ")
          out.println(s"$id\t${title.replaceAll("\\s+", " ")}\t$cats")
          out.flush()
        case _ => None
      }
      buffer.setLength(0)
    }
  }

  private[this] val pattern = """(?i)\[\[Category:(.*?)\]\]""".r
  private[this] def extractCategories(text:String):Seq[String] = pattern.findAllMatchIn(text).map{ m =>
    m.group(1).split("[|:]").head
  }.toSeq

  private[this] def help(message:String = ""):Nothing = {
    if(message.nonEmpty) {
      System.err.println(s"ERROR: $message")
    }
    System.err.println(
      s"""USAGE: sbt 'runMain ${getClass.getName.dropRight(1)} jawiki-YYYYMMDD-pages-articles.xml.bz2 topics.h2'
         |
       """.stripMargin)
    throw new IllegalStateException()
  }
}
