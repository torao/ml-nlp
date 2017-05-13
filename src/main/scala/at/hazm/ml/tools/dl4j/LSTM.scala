package at.hazm.ml.tools.dl4j

import java.io.File
import java.nio.charset.StandardCharsets
import java.util

import at.hazm.ml.io.Database._
import at.hazm.ml.io.{Database, using}
import at.hazm.ml.nlp._
import at.hazm.ml.nlp.knowledge.Wikipedia
import at.hazm.ml.tools._
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
import org.nd4j.linalg.lossfunctions.LossFunctions.LossFunction
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.util.Random

object LSTM {
  def main(args:Array[String]):Unit = try {
    val lstm = new LSTM(new File(args(0)))
    lstm.exec(new File(args(1)), new File(args(2)))
  } catch {
    case ex:Exception => ex.printStackTrace()
  }
}

class LSTM(file:File) {
  val logger = LoggerFactory.getLogger(getClass)

  private[this] val db = new Database(file)
  private[this] val corpus = new Corpus(db)
  private[this] val terms = new corpus.Vocabulary()
  private[this] val sentence = new db.KVS[Int]("sentences")
  private[this] val docsDB = new db.KVS[String]("docs")

  def exec(src:File, modelFile:File):Unit = {
    val sampleSentenceSize = 10000
    val maxSentencesPerDoc = 100
    makeCorpus(src)
    val model = calculate(modelFile, sampleSentenceSize, maxSentencesPerDoc)
    predict(model, sampleSentenceSize, maxSentencesPerDoc, 5)
  }

  private[this] def predict(model:MultiLayerNetwork, sampleSentenceSize:Int, maxSentencesPerDoc:Int, predictCount:Int):Unit = {
    def _sentence(i:Int):String = {
      sentence(i).split("\t").filter(_.nonEmpty).map { term =>
        val Array(t, p) = term.split(":", 2)
        if(t == "*") s" ______ " else t
      }.mkString
    }
    //Create input for initialization
    val numSamples = 5
    val initializationInput = Nd4j.zeros(numSamples, sampleSentenceSize, maxSentencesPerDoc)

    // ランダムにドキュメントを選択しその中のセンテンスシーケンスを取得
    val sb = (0 until numSamples).map { i =>
      def _select():Seq[Sentence] = {
        val (_, d) = docsDB.randomSelect(math.random())
        val doc = Document(d)
        val seqIndex = (doc.sentences.length * math.random()).toInt
        val seqLength = ((doc.sentences.length - seqIndex) * math.random()).toInt
        val sentences = doc.sentences.slice(seqIndex, seqIndex + seqLength)
        if(!sentences.forall(_.index < sampleSentenceSize) || sentences.length < 3) {
          _select()
        } else sentences
      }

      val ss = _select()
      ss.zipWithIndex.foreach { case (seq, j) =>
        initializationInput.putScalar(Array(i, seq.index, j), 1.0f)
      }
      new StringBuilder(ss.map(s => "> " + _sentence(s.index)).mkString("。\n"))
    }

    //Sample from network (and feed samples back into input) one character at a time (for all samples)
    //Sampling is done in parallel here
    model.rnnClearPreviousState()
    var output = model.rnnTimeStep(initializationInput)
    output = output.tensorAlongDimension(output.size(2) - 1, 1, 0) //Gets the last time step output

    for(i <- 0 until predictCount) {
      //Set up next input (single time step) by sampling from previous output
      val nextInput = Nd4j.zeros(numSamples, sampleSentenceSize)
      //Output is a probability distribution. Sample from this for each example we want to generate, and add it to the new input
      for(s <- 0 until numSamples) {
        val outputProbDistribution = for(j <- 0 until sampleSentenceSize) yield output.getDouble(s, j)
        val sampledCharacterIdx = sampleFromDistribution(outputProbDistribution.toArray)

        nextInput.putScalar(Array(s, sampledCharacterIdx), 1.0f) //Prepare next time step input
        sb(s).append("! " + _sentence(sampledCharacterIdx) + "。\n") //Add sampled character to StringBuilder (human readable output)
      }

      output = model.rnnTimeStep(nextInput) //Do one time step of forward pass
    }

    sb.map(_.toString).zipWithIndex.foreach { case (str, i) =>
      System.out.println("----- Sample " + i + " -----")
      System.out.println(str)
      System.out.println()
    }
  }

  private[this] def calculate(modelFile:File, sentenceSize:Int, maxSentencesPerDoc:Int):MultiLayerNetwork = {
    val lstmLayerSize = 200 //Number of units in each GravesLSTM layer
    val miniBatchSize = 32 //Size of mini batch to use when  training
    val exampleLength = 1000 //Length of each training example sequence to use. This could certainly be increased
    val tbpttLength = 50 //Length for truncated back propagation through time. i.e., do parameter updates ever 50 characters
    val numEpochs = 1 //Total number of training epochs
    val generateSamplesEveryNMinibatches = 10 //How frequently to generate samples from the network? 1000 characters / 50 tbptt length: 20 parameter updates per minibatch
    val nSamplesToGenerate = 4 //Number of samples to generate after each training epoch
    val nCharactersToSample = 300 //Length of each sample to generate
    val generationInitialization = null //Optional character initialization; a random character is used if null

    // Above is Used to 'prime' the LSTM with a character sequence to continue/complete.
    // Initialization characters must all be in CharacterIterator.getMinimalCharacterSet() by default

    val rng = new Random(12345)

    if(!modelFile.exists()) {

      // Get a DataSetIterator that handles vectorization of text into something we can use to train
      // our GravesLSTM network.
      val iter = new IT(docsDB, 100, sentenceSize, maxSentencesPerDoc)

      //Set up network configuration:
      logger.info(f"nIn: ${iter.inputColumns()}%,d, nOut: ${iter.totalOutcomes()}%,d")
      val conf:MultiLayerConfiguration = new NeuralNetConfiguration.Builder()
        .optimizationAlgo(OptimizationAlgorithm.STOCHASTIC_GRADIENT_DESCENT)
        .iterations(1)
        .learningRate(0.1)
        .rmsDecay(0.95)
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
      logger.info(s"SLTM model stored: ${modelFile.getName}")
      net
    } else {
      logger.info(s"loading LSTM model: ${modelFile.getName}")
      ModelSerializer.restoreMultiLayerNetwork(modelFile)
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

  /**
    * 指定された Extract 済み Wikipedia データ (id title content の TSV 圧縮形式) からコーパスを作成します。
    *
    * @param src Wikipedia データファイル
    */
  private[this] def makeCorpus(src:File):Unit = {
    def sdb(text:String):Int = sentence.getIds(text).headOption.getOrElse {
      val id = sentence.size
      sentence.set(id, text)
      id
    }

    if(docsDB.size == 0) {
      fileProgress(src, StandardCharsets.UTF_8, db) { line =>
        using(new CaboCha()) { cabocha =>

          def makeSentence(content:String):Seq[Seq[Token]] = {
            val tokens = Token.parse(normalize(Wikipedia.deleteParenthesis(content)))
            val sentences = splitSetsuzokuJoshi(splitSentence(tokens)).flatMap { tk =>
              val text = Wikipedia.deleteDokuten(tk).map(_.term).mkString
              val cs = cabocha.parse(text)
              val linkedChunkIds = cs.chunks.map(_.link).toSet
              cs.chunks.filter(c => !linkedChunkIds.contains(c.id)).map { leaf =>
                def getSequence(c:CaboCha.Chunk):Seq[CaboCha.Chunk] = {
                  if(c.link < 0) Seq(c) else {
                    c +: getSequence(cs.chunks.find(_.id == c.link).get)
                  }
                }

                getSequence(leaf).map(_.tokens.map(_.term).mkString).mkString
              }
            }
            sentences.map(Token.parse)
          }

          try {
            val Array(id, title, content) = line.split("\t")
            if(! title.endsWith("一覧") && ! title.contains("曖昧さ回避")){
              val splitTokens = makeSentence(content)
              val sentences = splitTokens.map { sentence =>
                val termIndices = terms.register(sentence.map{t => s"${t.term}:${t.pos1}" })
                val i = sdb(simplify(sentence).map { t => s"${t.term}:${t.pos}" }.mkString("\t"))
                val s = sentence.map { t => termIndices(s"${t.term}:${t.pos1}") }.toList
                Sentence(i, s)
              }
              val doc = Document(id.toInt, title, sentences)
              docsDB.set(doc.id.toString, doc.toString)
            }
          } catch {
            case ex:MatchError =>
              throw new Exception(s"unexpected source file format: $line", ex)
          }
        }
      }
    }
  }

  /** 構文の単純化 */
  private[this] def simplify(tokens:Seq[Token]):Seq[Token] = {
    val buffer = mutable.Buffer[Token]()
    for(i <- tokens.indices) {
      val t = tokens(i)
      if(t.pos1 == "名詞") {
        if(buffer.isEmpty || (buffer.last.term != "*" && buffer.last.pos1 != t.pos1)) {
          buffer.append(t.copy(term = "*", pos = t.pos1))
        }
      } else if(t.pos1 != "記号") {
        buffer.append(t)
      }
    }
    buffer
  }

  private[this] class IT(kvs:Database#KVS[String], miniBatchSize:Int, sentenceSize:Int, maxSentencesPerDoc:Int) extends DataSetIterator {
    private[this] val size = kvs.size
    private[this] var it:Option[Database.Cursor[(String, String)]] = None
    private[this] var remains = sentenceSize

    override def next(num:Int):DataSet = {
      val docs = it.get.take(num).map { case (_, serializedDoc) =>
        val sentences = Document(serializedDoc).sentences
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
        docs.zipWithIndex.foreach { case (sentences, i) =>
          sentences.zipWithIndex.foreach { case (s, pos) =>
            input.putScalar(Array(i, s.index, pos), 1.0)
            labels.putScalar(Array(i, s.index, pos), 1.0)
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

    override def hasNext:Boolean = {
      if(it.isEmpty) {
        it = Some(kvs.toCursor)
      }
      it.get.hasNext && remains > 0
    }
  }

  case class Sentence(index:Int, terms:Seq[Int]) {
    override def toString:String = s"$index:${terms.mkString(" ")}"
  }

  object Sentence {
    def apply(s:String):Sentence = {
      val Array(index, terms) = s.split(":", 2)
      Sentence(index.toInt, terms.split(" ").map(_.toInt).toSeq)
    }
  }

  case class Document(id:Int, title:String, sentences:Seq[Sentence]) {
    override def toString:String = {
      s"$id\t$title\t${sentences.map(_.toString).mkString(";")}"
    }
  }

  object Document {
    def apply(s:String):Document = try {
      val Array(id, title, sentences) = s.split("\t", 3)
      Document(id.toInt, title, sentences.split(";").filter(_.nonEmpty).map(Sentence.apply))
    } catch {
      case ex:MatchError =>
        throw new IllegalArgumentException(s"invalid document: $s", ex)
    }
  }

}
