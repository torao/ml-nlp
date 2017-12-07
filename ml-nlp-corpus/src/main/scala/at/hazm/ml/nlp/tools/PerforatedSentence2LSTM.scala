/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements; and to You under the Apache License, Version 2.0.
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package at.hazm.ml.nlp.tools

import java.io.File
import java.util

import at.hazm.core.db.Cursor
import at.hazm.ml.nlp.Corpus
import at.hazm.ml.nlp.corpus.PerforatedSentences
import at.hazm.ml.nlp.model.PerforatedDocument
import org.deeplearning4j.nn.api.OptimizationAlgorithm
import org.deeplearning4j.nn.conf.layers.{GravesLSTM, RnnOutputLayer}
import org.deeplearning4j.nn.conf.{BackpropType, MultiLayerConfiguration, NeuralNetConfiguration, Updater}
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork
import org.deeplearning4j.nn.weights.WeightInit
import org.deeplearning4j.optimize.listeners.ScoreIterationListener
import org.deeplearning4j.util.ModelSerializer
import org.nd4j.linalg.activations.Activation
import org.nd4j.linalg.dataset.DataSet
import org.nd4j.linalg.dataset.api.DataSetPreProcessor
import org.nd4j.linalg.dataset.api.iterator.DataSetIterator
import org.nd4j.linalg.factory.Nd4j
import org.nd4j.linalg.learning.config.RmsProp
import org.nd4j.linalg.lossfunctions.LossFunctions.LossFunction
import org.slf4j.LoggerFactory

import scala.collection.mutable

object PerforatedSentence2LSTM {
  private[this] val logger = LoggerFactory.getLogger(getClass)

  val DefaultSentenceSize:Int = 10000
  val DefaultMaxSentencesPerDoc:Int = 1000

  /*
  def exec(src:File, modelFile:File):Unit = {
    val sampleSentenceSize = 10000
    val maxSentencesPerDoc = 100
    //makeCorpus(src)
    val model = calculate(modelFile, sampleSentenceSize, maxSentencesPerDoc)
    predict(model, sampleSentenceSize, maxSentencesPerDoc, 5)
  }
  */

  def predict(corpus:Corpus, doc:PerforatedDocument, model:MultiLayerNetwork, predictCount:Int, sampleSentenceSize:Int = DefaultSentenceSize, maxSentencesPerDoc:Int = DefaultMaxSentencesPerDoc):Seq[Int] = {

    //Create input for initialization
    val inputSentences = doc.sentences.flatten
    val numSamples = inputSentences.length
    val initializationInput = Nd4j.zeros(numSamples, sampleSentenceSize, maxSentencesPerDoc)

    inputSentences.zipWithIndex.foreach { case (sentence, i) =>
      initializationInput.putScalar(Array(0, sentence, i), 1.0f)
    }

    //Sample from network (and feed samples back into input) one character at a time (for all samples)
    //Sampling is done in parallel here
    model.rnnClearPreviousState()
    var output = model.rnnTimeStep(initializationInput)
    output = output.tensorAlongDimension(output.size(2) - 1, 1, 0) //Gets the last time step output

    val sequel = mutable.Buffer[Int]()
    for(i <- 0 until predictCount) {
      //Set up next input (single time step) by sampling from previous output
      val nextInput = Nd4j.zeros(numSamples, sampleSentenceSize)
      //Output is a probability distribution. Sample from this for each example we want to generate, and add it to the new input
      for(s <- 0 until numSamples) {
        val outputProbDistribution = for(j <- 0 until sampleSentenceSize) yield output.getDouble(s, j)
        val sampledCharacterIdx = sampleFromDistribution(outputProbDistribution.toArray)

        nextInput.putScalar(Array(s, sampledCharacterIdx), 1.0f) //Prepare next time step input
        sequel.append(sampledCharacterIdx)
      }

      output = model.rnnTimeStep(nextInput) //Do one time step of forward pass
    }

    sequel
  }

  def train(corpus:Corpus, baseName:String, sentenceSize:Int = DefaultSentenceSize, maxSentencesPerDoc:Int = DefaultMaxSentencesPerDoc):MultiLayerNetwork = {
    logger.info(s"--- 最短文の連続学習 (LSTM) ---")
    val lstmLayerSize = 200 // Number of units in each GravesLSTM layer
    val miniBatchSize = 32 // Size of mini batch to use when  training
    val exampleLength = 1000 // Length of each training example sequence to use. This could certainly be increased
    val tbpttLength = 50 // Length for truncated back propagation through time. i.e., do parameter updates ever 50 characters
    val numEpochs = 1 // Total number of training epochs
    val generateSamplesEveryNMinibatches = 10 // How frequently to generate samples from the network? 1000 characters / 50 tbptt length: 20 parameter updates per minibatch
    val nSamplesToGenerate = 4 // Number of samples to generate after each training epoch
    val nCharactersToSample = 300 // Length of each sample to generate
    val generationInitialization = null // Optional character initialization; a random character is used if null

    // Above is Used to 'prime' the LSTM with a character sequence to continue/complete.
    // Initialization characters must all be in CharacterIterator.getMinimalCharacterSet() by default

    val iterations = 1
    val learningRate = 0.1

    // 保存先のファイル名を決定
    val id = s"sen${sentenceSize}mspd$maxSentencesPerDoc-l${lstmLayerSize}i${iterations}lr${(learningRate * 100).toInt}"
    val modelFile:File = new File(baseName + "_" + id + "-lstm-sentence.zip")

    if(!modelFile.exists()) {

      // Get a DataSetIterator that handles vectorization of text into something we can use to train
      // our GravesLSTM network.
      val iter = new PerforatedSentenceIterator(corpus.perforatedSentences, 100, sentenceSize, maxSentencesPerDoc)

      //Set up network configuration:
      logger.info(f"nIn: ${iter.inputColumns()}%,d, nOut: ${iter.totalOutcomes()}%,d")
      val conf:MultiLayerConfiguration = new NeuralNetConfiguration.Builder()
        .optimizationAlgo(OptimizationAlgorithm.STOCHASTIC_GRADIENT_DESCENT)
        .iterations(iterations)
        .learningRate(learningRate)
        .updater(new RmsProp(0.95))
        .seed(12345)
        .regularization(true)
        .l2(0.001)
        .weightInit(WeightInit.XAVIER)
        .updater(Updater.RMSPROP)
        .list()
        .layer(0, new GravesLSTM.Builder().nIn(iter.inputColumns()).nOut(lstmLayerSize)
          .activation(Activation.TANH).build())
        .layer(1, new GravesLSTM.Builder().nIn(lstmLayerSize).nOut(lstmLayerSize)
          .activation(Activation.TANH).build())
        .layer(2, new RnnOutputLayer.Builder(LossFunction.MCXENT).activation(Activation.SOFTMAX) //MCXENT + softmax for classification
          .nIn(lstmLayerSize).nOut(iter.totalOutcomes()).build())
        .backpropType(BackpropType.TruncatedBPTT)
        .tBPTTForwardLength(tbpttLength)
        .tBPTTBackwardLength(tbpttLength)
        .pretrain(false)
        .backprop(true)
        .build()

      val net = new MultiLayerNetwork(conf)
      net.init()
      net.setListeners(new ScoreIterationListener(1))

      //Print the  number of parameters in the network (and for each layer)
      val layers = net.getLayers
      var totalNumParams = 0
      for(i <- layers.indices) {
        val nParams = layers(i).numParams()
        System.out.println("Number of parameters in layer " + i + ": " + nParams)
        totalNumParams += nParams
      }
      System.out.println("Total number of network parameters: " + totalNumParams)

      //Do training, and then generate and print samples from network
      var miniBatchNumber = 0
      for(i <- 0 until numEpochs) {
        while(iter.hasNext()) {
          val ds = iter.next()
          net.fit(ds)
          miniBatchNumber += 1
          if(miniBatchNumber % generateSamplesEveryNMinibatches == 0) {
            System.out.println("--------------------")
            System.out.println(s"Completed $miniBatchNumber minibatches of size ${miniBatchSize}x$exampleLength characters")
            System.out.println(s"Sampling characters from network given initialization '${if(generationInitialization == null) "" else generationInitialization}'")
          }
        }

        iter.reset() //Reset iterator for another epoch
      }

      ModelSerializer.writeModel(net, modelFile, true)
      logger.info(s"LSTM モデルを保存しました: ${modelFile.getName}")
      net
    } else {
      logger.info(s"LSTM モデルをロードしています: ${modelFile.getName}")
      val model = ModelSerializer.restoreMultiLayerNetwork(modelFile)
      logger.info(s"LSTM モデルをロードしました: ${modelFile.getName}")
      model
    }
  }

  /** Given a probability distribution over discrete classes, sample from the distribution
    * and return the generated class index.
    *
    * @param distribution Probability distribution over classes. Must sum to 1.0
    */
  private[this] def sampleFromDistribution(distribution:Array[Double]):Int = {
    (0 until 10).foreach { _ =>
      val d = math.random()
      var sum = 0.0
      for(i <- distribution.indices) {
        sum += distribution(i)
        if(d <= sum) return i
      }
      //If we haven't found the right index yet, maybe the sum is slightly
      //lower than 1 due to rounding error, so try again.
    }
    //Should be extremely unlikely to happen if distribution is a valid probability distribution
    throw new IllegalArgumentException(s"Distribution is invalid?")
  }

  private[this] class PerforatedSentenceIterator(kvs:PerforatedSentences, miniBatchSize:Int, sentenceSize:Int, maxSentencesPerDoc:Int) extends DataSetIterator {
    private[this] val size = kvs.documentSize
    private[this] var it:Option[Cursor[(Int, PerforatedDocument)]] = None
    private[this] var remains = sentenceSize

    override def hasNext:Boolean = {
      if(it.isEmpty) {
        it = Some(kvs.toDocumentCursor)
      }
      it.get.hasNext && remains > 0
    }

    override def next(num:Int):DataSet = {

      // num 文書分の perforated sentences を取得
      val docs = it.get.take(num).map { case (_, doc) =>
        val sentences = doc.sentences.flatten
        if(sentences.length > remains) {
          remains = 0
          sentences.take(remains)
        } else {
          remains -= sentences.length
          sentences
        }
      }.filter(_.nonEmpty).toList

      if(docs.isEmpty) {
        next(num)
      } else {
        val input = Nd4j.create(Array(docs.length, sentenceSize, maxSentencesPerDoc), 'f')
        val labels = Nd4j.create(Array(docs.length, sentenceSize, maxSentencesPerDoc), 'f')
        docs.zipWithIndex.foreach { case (sentences, docIndex) =>
          sentences.take(maxSentencesPerDoc).zipWithIndex.foreach { case (s, pos) =>
            input.putScalar(Array(docIndex, s, pos), 1.0)
            labels.putScalar(Array(docIndex, s, pos), 1.0)
          }
        }
        new DataSet(input, labels)
      }
    }

    override def batch():Int = miniBatchSize

    override def cursor():Int = it.map(_.count.toInt).getOrElse(0)

    override def totalExamples():Int = (size - 1) / miniBatchSize - 2

    override def resetSupported():Boolean = true

    override def inputColumns():Int = sentenceSize

    override def getPreProcessor:DataSetPreProcessor = throw new UnsupportedOperationException("Not implemented")

    override def setPreProcessor(preProcessor:DataSetPreProcessor):Unit = throw new UnsupportedOperationException("Not implemented")

    override def getLabels:util.List[String] = ???

    override def totalOutcomes():Int = sentenceSize

    override def reset():Unit = {
      it.foreach(_.close())
      it = None
      remains = sentenceSize
    }

    override def asyncSupported():Boolean = false

    override def numExamples():Int = totalExamples()

    override def next():DataSet = next(miniBatchSize)
  }

}
