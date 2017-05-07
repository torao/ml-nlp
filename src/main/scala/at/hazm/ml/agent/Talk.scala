package at.hazm.ml.agent

import java.io.File

import at.hazm.ml.nlp.Token
import com.typesafe.config.ConfigFactory

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Try

class Talk(file:File) {
  private[this] val talk = if(file.isFile) ConfigFactory.parseFile(file) else ConfigFactory.empty()

  private[this] val cyclopediaSearch = talk.getStringList("factoid.cyclopedia").asScala.map(tokenize)

  def recognize(text:String):Reply = {
    val talks = Token.parse(text)
    ???
  }

  def cyclopedia(talk:List[Token]):Option[String] = {
    cyclopediaSearch.foreach { pattern =>
      val variable = mutable.HashMap[String,String]()
      for(i <- 0 until talk.length - pattern.length) {
        variable.clear()
        for(j <- pattern.indices){
          if(pattern(j).pos1 == "$変数"){
            variable.put(pattern(j).term, talk(i+j).term)
          } else if(pattern(j).term == talk(i+j).term){

          }
        }
      }
    }
    ???
  }

  private[this] def tokenize(text:String):List[Token] = {
    val tokens = Token.parse(text)
    if(tokens.length < 2) List.empty else {
      tokens.sliding(2).map { set =>
        if(set.head.term == "$" && Try(set(1).term.toInt).isSuccess) {
          set.head.copy(term = "$" + set(1).term, pos = "$変数")
        } else set.head
      }.toList :+ tokens.last
    }
  }

}
