package at.hazm.ml.nlp.knowledge

import java.io.{BufferedReader, File}
import java.nio.charset.StandardCharsets

import at.hazm.ml.nlp.SentenceIterator
import at.hazm.ml.io.openTextInput

class WikipediaIterator(file:File) extends SentenceIterator {
  private[this] var in:BufferedReader = _
  private[this] var line:String = _

  override def reset():Unit = {
    close()
    in = openTextInput(file, StandardCharsets.UTF_8, 8 * 1024)
    line = in.readLine()
  }

  override def close():Unit = {
    if(in != null){
      in.close()
      line = null
    }
  }

  override def hasNext:Boolean = line != null

  override def next():String = if(line != null){
    val Array(_, _, content) = line.split("\t", 3)
    content
  } else {
    throw new NoSuchElementException("empty iterator")
  }
}
