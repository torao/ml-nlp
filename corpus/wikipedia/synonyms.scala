import java.io._
import java.nio.charset.StandardCharsets
import java.util.zip.{GZIPInputStream, GZIPOutputStream}
import javax.xml.XMLConstants
import javax.xml.parsers.SAXParserFactory

import org.xml.sax.{Attributes, SAXException}
import org.xml.sax.helpers.DefaultHandler

/*
 * Wikipedia の xxwiki-yyyymmdd-pages-articles.xml からシノニムとして使用できる REDIRECT を抽出し、
 * [term]{:qualifier}[TAB][term]{:qualifier} 形式の TSV を出力する。
 *
 * USAGE: scala synonyms.scala [infile].xml.gz [outfile].tsv
 */

val factory = SAXParserFactory.newInstance()
factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, false)
val parser = factory.newSAXParser()


val is = new BufferedInputStream(new GZIPInputStream(new FileInputStream(args(0))), 512 * 1024)
val out = new PrintWriter(new OutputStreamWriter(new FileOutputStream(args(1)), StandardCharsets.UTF_8))

val redirect = """#REDIRECT\s*\[\[(.*?)\]\]""".r.pattern
val paren = "(.*)\\s*\\((.*)\\)\\s*".r
def norm(t:String):String = t.replaceAll("[\\s　]+", " ").replaceAll("（", "(").replaceAll("）", ")").toUpperCase.trim()
def conv(t:String):String = t match {
  case paren(x, q) => x.trim() + ":" + q.trim()
  case x => x
}
val prefix = Seq("WIKIPEDIA:", "CATEGORY:", "PORTAL:", "HELP:")
def parse(title:String, text:String):Unit = {
  val m = redirect.matcher(text)
  while(m.find()){
    val red = m.group(1).split("#", 2).head.trim()
    if(Seq(title, red).forall{ t => prefix.forall{ p => ! t.startsWith(p) } && t.nonEmpty } && title != red){
      out.write(s"${conv(title)}\t${conv(red)}\n")
      out.flush()
    }
  }
}

parser.parse(is, new DefaultHandler{
  var title:Option[String] = None
  val buffer = new StringBuilder()
  var capture = false
  override def startElement(uri:String, localName:String, qName:String, attributes:Attributes) {
    if(qName == "text" || qName == "title"){
      capture = true
      buffer.length = 0
    }
  }
  override def endElement(uri:String, localName:String, qName:String) {
    if(qName == "text"){
      parse(norm(title.get), norm(buffer.toString()))
      capture = false
      buffer.length = 0
      title = None
    } else if(qName == "title"){
      title = Some(buffer.toString())
      capture = false
      buffer.length = 0
    }
  }
  override def characters(ch:Array[Char], start:Int, length:Int) {
    if(capture){
      buffer.appendAll(ch, start, length)
    }
  }
}, args(0))

out.close()
is.close()