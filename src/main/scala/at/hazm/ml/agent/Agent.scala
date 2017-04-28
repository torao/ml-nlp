package at.hazm.ml.agent

import java.io.File

import at.hazm.ml.io.Database
import at.hazm.ml.nlp.Token
import at.hazm.ml.nlp.knowledge.Knowledge
import org.slf4j.LoggerFactory
import twitter4j.Status

class Agent(dir:File, tones:Seq[Tone] = Seq.empty) {
  import Agent.logger

  private[this] val db = new Database(new File(dir, "agent.db"))
  private[this] val knowledge = new Knowledge(new File("brain.db"))

  private[this] val channels = Seq(Twitter, Console)

  private object Twitter extends Channel.Twitter(new File(dir, "twitter4j.properties"), db){
    val MaxLength = 140
    override def ask(question:Status):Option[String] = {
      val text = question.getText.replaceAll("@[a-zA-Z0-9_]+", "").trim()
      val response = recognize(text)
      logger.info(s"from ${question.getUser.getScreenName}, ${question.getText} => $response")
      Some(s"@${question.getUser.getScreenName} ${changeTone(response)}")
    }
  }

  private object Console extends Channel.ConsoleChannel {
    override def ask(question:String):Option[String] = {
      val response = recognize(question)
      logger.info(s"$question => $response")
      Some(changeTone(response))
    }
  }

  def start():Unit = channels.foreach(_.start())
  def stop():Unit = channels.foreach(_.stop())

  def recognize(text:String):String = knowledge.findCyclopedia(text) match {
    case Seq() =>
      logger.debug(knowledge.findSynonyms(text).mkString("[", ", ", "]"))
      s"$text とは何でしょうか。"
    case Seq(term) => term.meanings
    case many =>
      many.find(_.qualifier.isEmpty) match {
        case Some(t) =>
          t.meanings + s" この他にも${many.filter(_.qualifier.nonEmpty).map(_.qualifier).mkString("「", "」「", "」")}に情報があります。"
        case None =>
          many.head.meanings + s" この他にも${many.tail.map(_.qualifier).mkString("「", "」「", "」")}に情報があります。"
      }
  }

  def changeTone(text:String):String = {
    tones.foldLeft(Token.parse(text)){ (tokens, tone) => tone.talk(tokens) }.map(_.term).mkString
  }
}

object Agent {
  private[Agent] val logger = LoggerFactory.getLogger(classOf[Agent])

  def main(args:Array[String]):Unit = {
    val agent = new Agent(new File("agents/entrezyx/"), Seq(Tone.DESU_MASU))
    agent.start()
    synchronized{ wait() }
    agent.stop()

  }
}