package at.hazm.ml.agent

import java.io.File

import at.hazm.core.db.LocalDB
import at.hazm.ml.nlp.Token
import at.hazm.ml.nlp.knowledge.Knowledge
import org.slf4j.LoggerFactory
import twitter4j.Status

class Agent(dir:File, tones:Seq[Tone] = Seq.empty) {
  import Agent.logger

  private[this] val db = new LocalDB(new File(dir, "agent.db"))
  private[this] val knowledge = new Knowledge(new File("brain.db"))
  private[this] val talk = new Talk(new File(dir, "talk.conf"))

  private[this] val channels = Seq(Twitter, Console)

  private object Twitter extends Channel.Twitter(new File(dir, "twitter4j.properties"), db){
    val MaxLength = 140
    override def ask(question:Status):Option[Reply] = {
      val text = question.getText.replaceAll("@[a-zA-Z0-9_]+", "").trim()
      val res = recognize(text)
      logger.info(s"from ${question.getUser.getScreenName}, ${question.getText} => ${res.response}")
      Some(res.copy(response = "@" + question.getUser.getScreenName + " " + changeTone(res.response)))
    }
  }

  private object Console extends Channel.ConsoleChannel {
    override def ask(question:String):Option[Reply] = {
      val res = recognize(question)
      Some(res.copy(response = changeTone(res.response)))
    }
  }

  def start():Unit = channels.foreach(_.start())
  def stop():Unit = channels.foreach(_.stop())

  def recognize(text:String):Reply = knowledge.findCyclopedia(text) match {
    case Seq() =>
      logger.debug(knowledge.findSynonyms(text).mkString("[", ", ", "]"))
      Reply(s"$text とは何でしょうか。", None)
    case Seq(term) =>
      Reply(term.meanings, term.url)
    case many =>
      many.find(_.qualifier.isEmpty).map { t =>
        Reply(t.meanings + s" この他にも${many.filter(_.qualifier.nonEmpty).map(_.qualifier).mkString("「", "」「", "」")}に情報があります。", t.url)
      }.getOrElse{
        val t = many.head
        Reply(t.meanings + s" この他にも${many.tail.map(_.qualifier).mkString("「", "」「", "」")}に情報があります。", t.url)
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