package at.hazm.ml.nlp.word2vec

import java.io.File

import at.hazm.ml.nlp.dl4j._
import at.hazm.ml.nlp.pipeline.Pipeline
import org.deeplearning4j.models.embeddings.loader.WordVectorSerializer
import org.deeplearning4j.models.word2vec.{Word2Vec => DL4JW2V}
import org.deeplearning4j.text.tokenization.tokenizerfactory.DefaultTokenizerFactory

import scala.collection.JavaConverters._

class Word2Vec private(model:DL4JW2V) {

  def nearest(term:Int, accuracy:Double):Map[Int, Double] = {
    model.accuracy(model.similarWordsInVocabTo(term.toString, accuracy)).asScala.map(x => (x._1.toInt, x._2.toDouble)).toMap
  }
}

object Word2Vec {

  def create(file:File, pipeline:Pipeline[Seq[Int], _]):Word2Vec = {
    val it = pipeline.asSentenceIterator
    val tf = new DefaultTokenizerFactory()

    val model = new DL4JW2V.Builder()
      .minWordFrequency(5)
      .iterations(1)
      .layerSize(100)
      .seed(42)
      .windowSize(5)
      .iterate(it)
      .tokenizerFactory(tf)
      .build
    model.fit()

    WordVectorSerializer.writeWord2VecModel(model, file)
    new Word2Vec(model)
  }

  def load(file:File):Word2Vec = {
    val model = WordVectorSerializer.readWord2VecModel(file)
    new Word2Vec(model)
  }
}