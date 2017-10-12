/* scala combine.scala wikipedia/ jawiki-20170420-pagers-articles.tsv.gz
 * wikiextract によって抽出され wikipedia/AA/ のようなディレクトリに分割保存された内容を結合する。
*/

import java.io._
import scala.io._
import java.util.zip._

val dir = new File(args(0))

def listFiles(parent:File = dir):List[File] = {
  parent.listFiles.flatMap{
    case file if file.isFile => Nil
    case dir if dir.isDirectory =>
      dir.listFiles.filter(_.isFile).toList ::: listFiles(dir)
  }.toList
}

//val out = new BufferedWriter(new OutputStreamWriter(System.out))
val out = new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(args(1))), "UTF-8"))
out.write("PAGE-ID\tTITLE\tCONTENT")

def flush(out:Writer, id:Int, title:String, text:String):Unit = out.write(s"$id\t$title\t$text\n")

val ATTR = """([a-zA-Z0-9-_]*)\s*=\s*"(.*?)"""".r
def attr(elem:String):Map[String,String] = {
  val a = scala.collection.mutable.HashMap[String,String]()
  val m = ATTR.pattern.matcher(elem)
  while(m.find()){
    a += (m.group(1) -> m.group(2))
  }
  a.toMap
}

val fileList = listFiles()
fileList.zipWithIndex.foreach{ case (file, i) =>
  val buffer = new StringBuilder()
  var id:Option[Int] = None
  var title:Option[String] = None
  var lineNum = -1
  try {
    Source.fromFile(file, "UTF-8").getLines.zipWithIndex.foreach{ case (line, num) =>
      lineNum = num
      line.trim() match {
        case begin if begin.startsWith("<doc") && Character.isWhitespace(begin.charAt(4)) =>
          // WSDL の通信サンプル <document> に反応する
          assert(id.isEmpty && title.isEmpty && buffer.toString.trim().isEmpty, s"$file(${num+1}): $line ($id, $title, $buffer)")
          val a = attr(begin)
          id = a.get("id").map(_.toInt)
          title = a.get("title").map(_.replaceAll("[\\s\u3000]+", " ").trim())
          buffer.length = 0
        case "</doc>" =>
          assert(id.nonEmpty && title.nonEmpty && buffer.nonEmpty, s"$file(${num+1}): $line ($id, $title, $buffer)")
          flush(out, id.get, title.get, buffer.toString)
          id = None
          title = None
          buffer.length = 0
        case text =>
          if(buffer.nonEmpty){
            buffer.append(" ")
          }
          buffer.append(text.replaceAll("[\\s\u3000]+", " ").trim())
      }
    }
  } catch {
    case ex:Throwable =>
      System.err.println(s"$file($lineNum): $ex")
      ex.printStackTrace()
  } finally {
    if(i % 100 == 0){
      System.err.println(f"$file ($lineNum%,d lines) ${i+1}%,d / ${fileList.size}%,d): SUCCESS")
    }
  }
}
out.close()
println(fileList.size)
