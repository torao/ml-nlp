package at.hazm.ml.tools

import java.io.{File, PushbackReader, StringReader}

import at.hazm.ml.io.{readText, writeText}
import at.hazm.ml.nlp.knowledge.Knowledge
import at.hazm.ml.tools.cmd.CommandError
import jline.console.ConsoleReader

object Brain {

  def main(args:Array[String]):Unit = {
    val knowledge = new Knowledge(new File("brain.db"))

    lazy val commands = Seq(
      cmd.Tokenize("tokenize", knowledge),
      cmd.Search("search", knowledge),
      cmd.Synonyms("synonyms", knowledge),
      cmd.Import("import", knowledge),
      cmd.Shell("!", knowledge),
      Help("help", knowledge)
    )
    case class Help(key:String, knowledge:Knowledge) extends cmd.Command{
      val help = ""
      def exec(console:ConsoleReader, args:List[String]):Unit = {
        commands.sortBy(_.key).foreach(c => System.out.println(s"${c.key} ${c.help.split("\n").find(_.nonEmpty).getOrElse("").trim}"))
        console.println(s"quit")
      }
    }

    // コンソールの構築と入力履歴の読み出し
    val console = new ConsoleReader()
    val history = new File(".brain_history")
    if(history.isFile) {
      val commands = readText(history)(in => Iterator.continually(in.readLine()).takeWhile(_ != null).toList)
      (if(commands.length > 100) {
        writeText(history) { out =>
          val c100 = commands.takeRight(100)
          c100.foreach(out.println)
          c100
        }
      } else {
        commands
      }).foreach { line => console.getHistory.add(line) }
    }

    Iterator
      .continually(console.readLine("brain> "))
      .takeWhile(line => line != null && line.trim.toLowerCase != "quit")
      .map { c =>
        writeText(history, append = true)(_.println(c))
        c.split("\\s+", 2).toList.filter(_.nonEmpty)
      }.foreach {
      case command :: params =>
        commands.find(_.key == command) match {
          case Some(cmd) =>
            try{
              cmd.exec(console, params.headOption.map(split).getOrElse(Nil))
            } catch {
              case ex:CommandError =>
                System.err.println(s"ERROR: ${ex.getMessage}")
              case ex:Exception =>
                System.err.println(s"ERROR: unhandled command error: $command $params")
                ex.printStackTrace()
            }
          case None =>
            System.err.println(s"ERROR: unknown command: $command")
        }
      case Nil => ()
    }
  }

  private[this] case class QuoteNotEnd(quote:Char) extends Exception
  private[this] case class ParseException(msg:String) extends Exception(msg)

  private[this] def split(text:String):List[String] = {
    val in = new PushbackReader(new StringReader(text))
    def mustRead():Char = in.read() match {
      case eof if eof < 0 => throw ParseException(s"")
      case ch => ch.toChar
    }
    def parse(buffer:StringBuilder = new StringBuilder(), quote:Char = '\0'):List[String] = in.read() match {
      case eof if eof < 0 =>
        if(quote != '\0'){
          throw QuoteNotEnd(quote)
        }
        if(buffer.nonEmpty) List(buffer.toString()) else Nil
      case ch @ ('\"' | '\'') =>
        if(ch == quote){
          val param = buffer.toString()
          buffer.clear()
          param :: parse(buffer)
        } else {
          parse(buffer, quote = ch.toChar)
        }
      case ch if Character.isWhitespace(ch) && quote == '\0' =>
        if(buffer.nonEmpty){
          val param = buffer.toString()
          buffer.clear()
          param :: parse(buffer)
        } else {
          parse(buffer)
        }
      case '\\' if quote != '\0' =>
        mustRead() match {
          case 'b' => buffer.append('\b')
          case 'n' => buffer.append('\n')
          case 'r' => buffer.append('\r')
          case 't' => buffer.append('\t')
          case '\'' => buffer.append('\'')
          case '\"' => buffer.append('\"')
          case '\\' => buffer.append('\\')
          case o if o >= '0' && o <= '7' =>
            buffer.append(Integer.parseInt(s"$o${mustRead()}${mustRead()}", 8).toChar)
          case 'u' =>
            buffer.append(Integer.parseInt((0 until 4).map(_ => mustRead()).mkString, 18).toChar)
          case unexpected =>
            throw ParseException(s"$unexpected")
        }
        parse(buffer, quote)
      case ch =>
        buffer.append(ch.toChar)
        parse(buffer, quote)
    }
    parse()
  }

}
