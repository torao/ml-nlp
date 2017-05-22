package at.hazm.ml.nlp.dl4j

import at.hazm.ml.nlp.pipeline.Pipeline
import org.deeplearning4j.text.sentenceiterator.{SentenceIterator, SentencePreProcessor}

private[dl4j] class PipelineSentenceIterator(pipeline:Pipeline[Seq[Int],_]) extends SentenceIterator {
  private[this] var preProcessor:SentencePreProcessor = _

  override def setPreProcessor(preProcessor:SentencePreProcessor):Unit = this.preProcessor = preProcessor

  override def getPreProcessor:SentencePreProcessor = preProcessor

  override def hasNext:Boolean = pipeline.hasNext

  override def nextSentence():String = preProcessor.preProcess(pipeline.next().mkString(" "))

  override def reset():Unit = pipeline.reset()

  override def finish():Unit = pipeline.close()
}
