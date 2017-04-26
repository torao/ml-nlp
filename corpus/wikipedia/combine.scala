import java.io._
import java.util.zip._
import scala.io._

val dir = new File(".")

def listFiles(parent:File = new File(".")):List[File] = {
  parent.listFiles.flatMap{
    case file if file.isFile => Nil
    case dir if dir.isDirectory =>
      dir.listFiles.filter(_.isFile).toList ::: listFiles(dir)
  }.toList
}

val out = new BufferedWriter(new OutputStreamWriter(System.out))
//val out = new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream("jawiki-latest-pagers-articles2.tsv.gz")), "UTF-8"))

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

listFiles().foreach{ file =>
  val buffer = new StringBuilder()
  var id:Option[Int] = None
  var title:Option[String] = None
  var lineNum = -1;
  try {
    Source.fromFile(file, "UTF-8").getLines.zipWithIndex.foreach{ case (line, num) =>
      lineNum = num
      line.trim() match {
        case begin if begin.startsWith("<doc") =>
          assert(id.isEmpty && title.isEmpty && buffer.toString.trim().isEmpty, s"$file(${num+1}): $line ($id, $title, $buffer)")
          val a = attr(begin)
          id = a.get("id").map(_.toInt)
          title = a.get("title").map(_.replaceAll("[\\s　]+", " ").trim())
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
          buffer.append(text.replaceAll("\\s+", " ").trim())
      }
    }
  } catch {
    case ex:Throwable =>
      System.err.println(s"$file($lineNum): $ex")
      ex.printStackTrace()
  } finally {
    System.err.println(s"$file($lineNum): SUCCESS")
  }
}
out.close()
println(listFiles().size)
