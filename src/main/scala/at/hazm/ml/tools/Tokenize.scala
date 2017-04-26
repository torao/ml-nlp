package at.hazm.ml.tools

import at.hazm.ml.nlp.Token
import at.hazm.ml.nlp.normalize
import jline.console.ConsoleReader

object Tokenize {
  def main(args:Array[String]):Unit = {
    val console = new ConsoleReader()
    Iterator.continually(console.readLine("tokenizer> ")).takeWhile(line => line != null && line.trim.toLowerCase != "quit").foreach { line =>
      val tokens = Token.parse(normalize(line))
      val table = tokens.zipWithIndex.map{ case (token, i) =>
        List((i+1).toString, token.term, token.base.getOrElse(""), token.pos, token.inflection.getOrElse(""), token.reading.getOrElse(""))
      }
      if(table.nonEmpty){
        def len(t:String):Int = t.getBytes("Shift_JIS").length
        val size = for(x <- table.head.indices) yield table.map(c=>len(c(x))).max
        table.map(_.map{ case "" => "-"; case x => x }).foreach{ row =>
          def r(i:Int) = " " * (size(i)-len(row(i))) + row(i)
          def l(i:Int) = row(i) + " " * (size(i)-len(row(i)))
          System.out.println(s"  ${r(0)}. ${l(1)} ${l(2)} ${l(3)} ${l(4)} ${l(5)}")
        }
      }
    }
  }

}
