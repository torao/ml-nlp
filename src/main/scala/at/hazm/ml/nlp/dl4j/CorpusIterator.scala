package at.hazm.ml.nlp.dl4j

import at.hazm.core.db.Cursor
import at.hazm.ml.nlp.{Corpus, Paragraph}
import org.deeplearning4j.text.sentenceiterator.{SentenceIterator, SentencePreProcessor}

class CorpusIterator(corpus:Corpus) extends SentenceIterator {
  private[this] var preProcessor:Option[SentencePreProcessor] = None
  private[this] var iterator:Option[Cursor[(Int,Paragraph)]] = None

  override def setPreProcessor(preProcessor:SentencePreProcessor):Unit = this.preProcessor = Option(preProcessor)

  override def getPreProcessor:SentencePreProcessor = preProcessor.orNull

  override def hasNext:Boolean = {
    if(iterator.isEmpty){
      iterator = Some(corpus.paragraphs.toCursor)
    }
    iterator.get.hasNext
  }

  override def nextSentence():String = iterator.get.next()._2.toIndices.mkString(" ")

  override def reset():Unit = {
    finish()
    iterator = Some(corpus.paragraphs.toCursor)
  }

  override def finish():Unit = {
    iterator.foreach(_.foreach(_ => ()))
    iterator = None
  }
}
