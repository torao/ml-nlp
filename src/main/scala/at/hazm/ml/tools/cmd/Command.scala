package at.hazm.ml.tools.cmd

import at.hazm.ml.nlp.knowledge.Knowledge
import jline.console.ConsoleReader

trait Command {
  val key:String
  val knowledge:Knowledge
  val help:String

  @throws[CommandError]
  def exec(console:ConsoleReader, args:List[String]):Unit
}
