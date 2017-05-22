package at.hazm.ml.nlp

import at.hazm.ml.nlp.pipeline.Pipeline

package object dl4j {

  implicit class _Pipeline(pipeline:Pipeline[Seq[Int], _]) {
    def asSentenceIterator = new PipelineSentenceIterator(pipeline)
  }

}
