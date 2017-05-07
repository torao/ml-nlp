package at.hazm.ml.tools

import at.hazm.ml.nlp.knowledge.{Knowledge, Synonym}
import at.hazm.ml.nlp.{Token, normalize}
import jline.console.ConsoleReader

import scala.collection.mutable
import scala.io.Source

package object cmd {

  case class CommandError(msg:String) extends Exception(msg)

  case class Tokenize(key:String, knowledge:Knowledge) extends Command {
    val help:String =
      """
        |
    """.stripMargin

    def exec(console:ConsoleReader, args:List[String]):Unit = args.foreach { sentence =>
      val tokens = Token.parse(normalize(sentence))
      val table = tokens.zipWithIndex.map { case (token, i) =>
        List((i + 1).toString, token.term, token.base.getOrElse(""), token.pos, token.inflection.getOrElse(""), token.reading.getOrElse(""))
      }
      if(table.nonEmpty) {
        def len(t:String):Int = t.getBytes("Shift_JIS").length

        val size = for(x <- table.head.indices) yield table.map(c => len(c(x))).max
        table.map(_.map { case "" => "-"; case x => x }).foreach { row =>
          def r(i:Int) = " " * (size(i) - len(row(i))) + row(i)

          def l(i:Int) = row(i) + " " * (size(i) - len(row(i)))

          System.out.println(s"  ${r(0)}. ${l(1)} ${l(2)} ${l(3)} ${l(4)} ${l(5)}")
        }
      }
    }

  }

  case class Search(key:String, knowledge:Knowledge) extends Command {
    val help:String =
      """
        |
    """.stripMargin

    def exec(console:ConsoleReader, args:List[String]):Unit = args.foreach { term =>
      knowledge.cyclopedia.search(normalize(term.mkString(" "))).sortBy(_.qualifier).foreach { term =>
        if(term.qualifier.isEmpty) {
          System.out.println(s"  ${term.meanings}")
        } else {
          System.out.println(s"    (${term.qualifier}) ${term.meanings}")
        }
      }
    }

  }

  case class Synonyms(key:String, knowledge:Knowledge) extends Command {
    val help:String =
      """
        |
    """.stripMargin

    def exec(console:ConsoleReader, args:List[String]):Unit = args.foreach { term =>
      val search = normalize(term)
      val entities = mutable.HashSet[String]()
      val aliases = mutable.Buffer[Synonym.Term]()
      knowledge.cyclopedia.search(search).sortBy(_.qualifier).foreach{ t =>
        entities.add(t.name)
        aliases.appendAll(knowledge.synonyms.reverseSearch(t.term))
      }
      knowledge.synonyms.search(search) match {
        case Seq() =>
          //System.out.println(search)
          aliases.appendAll(knowledge.synonyms.reverseSearch(search))
        case syns =>
          syns.sortBy(_.qualifier).foreach { term =>
            entities.add(term.alterName)
            aliases.appendAll(knowledge.synonyms.reverseSearch(term.alterTerm))
          }
      }
      entities.toSeq.sorted.foreach(System.out.println)
      aliases.groupBy(_.id).values.map(_.head).toSeq.sortBy(_.name)foreach{ t =>
        System.out.println(f"${t.id}%8d. ${t.name}")
      }
    }

  }

  case class Shell(key:String, knowledge:Knowledge) extends cmd.Command{
    val help = "[command]"
    def exec(console:ConsoleReader, args:List[String]):Unit = {
      val proc = Runtime.getRuntime.exec(args.toArray)
      val in = proc.getInputStream
      val thread = new Thread(args.mkString(" ")){
        override def run():Unit = Source.fromInputStream(in).getLines().foreach(console.println)
      }
      thread.setDaemon(true)
      thread.start()
      proc.waitFor()
    }
  }

}
