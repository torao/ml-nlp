package at.hazm.ml.tools.dl4j

import java.io._
import java.text.BreakIterator
import java.util
import java.util.zip.GZIPInputStream

import at.hazm.ml.nlp.pipeline.IntakeFilter.Cache
import at.hazm.ml.nlp.{Corpus, Token}
import at.hazm.ml.tools._
import org.deeplearning4j.models.embeddings.loader.WordVectorSerializer
import org.deeplearning4j.models.word2vec.Word2Vec
import org.deeplearning4j.text.sentenceiterator.BaseSentenceIterator
import org.deeplearning4j.text.tokenization.tokenizer.{TokenPreProcess, Tokenizer}
import org.deeplearning4j.text.tokenization.tokenizerfactory.TokenizerFactory

import scala.collection.JavaConverters._
import scala.collection.mutable

object W2V extends App {

  val corpus = new Corpus(new File(args(0) + "_corpus.db"), "word2vec")
  val vocab = corpus.vocabulary
  val docs = new File(args(0) + "_docs.txt.bz2")
  val cache = Cache(docs, { idx:Seq[Int] => idx.mkString(" ") }, { line:String => line.split(" ").collect { case x if x.nonEmpty => x.toInt }.toSeq })

  /*
  val pipeline = new Pipeline.Builder(
    new Source.TextLine(new File(args(0))),
    Destination.TextLine(new OutputStreamWriter(System.out))
  )
    .forward(Normalize())
    .forward(CaboChaTokenize())
    .forward(Flatten[Seq[CaboCha.Sentence], CaboCha.Sentence]())
    .forward { s:CaboCha.Sentence =>
      s.chunks.flatMap { c => c.tokens.map { t => s"${t.term}:${t.feature}" } }
    }
    .forward(vocab.Pipe)
    .forward(cache)
    .backward{ terms:Seq[String] => terms.mkString }
    .backward(vocab.Pipe)
    .build
  pipeline
  */

  /*
  val engine = UimaSentenceIterator.segmenter()
  val it = UimaSentenceIterato r.createWithPath("sample_5000.txt")
  it.setPreProcessor((s:String)=> Wikipedia.normalize(s))
  while(it.hasNext){
    System.out.println(it.nextSentence())
  }
  */

  val (src, dest) = args.toList match {
    case s :: d :: _ => (new File(s), new File(d))
    case _ =>
      System.err.println(s"USAGE: ${getClass.getName.dropRight(1)} [docid-terms.tsv.gz] [corpus]")
      System.exit(1)
      throw new Exception()
  }

  val model = if(!dest.exists()) {
    Token.parse("")
    progress(src.getName, countLines(src)) { prog =>
      val it = new SI(src, prog)
      val vec = new Word2Vec.Builder()
        .minWordFrequency(5)
        .iterations(1)
        .layerSize(100)
        .seed(42)
        .windowSize(5)
        .iterate(it)
        .tokenizerFactory(new TKF())
        .build()

      vec.fit()
      it.finish()

      WordVectorSerializer.writeWord2VecModel(vec, dest)
      vec
    }
  } else {
    WordVectorSerializer.readWord2VecModel(dest)
  }

  class TK(sentence:String, defaultTPP:Option[TokenPreProcess]) extends Tokenizer {
    private[this] var preProcess = defaultTPP
    private[this] val tokens = Token.parse(sentence).map { t => s"${t.base}:${t.pos.split("-").head}" }
    private[this] val it = tokens.toIterator

    override def hasMoreTokens:Boolean = it.hasNext

    override def countTokens():Int = tokens.size

    override def nextToken():String = {
      preProcess.map {
        _.preProcess(it.next())
      }.getOrElse(it.next)
    }

    override def setTokenPreProcessor(tokenPreProcessor:TokenPreProcess):Unit = {
      preProcess = Option(tokenPreProcessor)
    }

    override def getTokens:util.List[String] = tokens.asJava
  }

  class TKF extends TokenizerFactory {
    private[this] var preProcess:Option[TokenPreProcess] = None

    override def getTokenPreProcessor:TokenPreProcess = preProcess.orNull

    override def setTokenPreProcessor(preProcessor:TokenPreProcess):Unit = {
      preProcess = Option(preProcessor)
    }

    override def create(toTokenize:String):Tokenizer = new TK(toTokenize, preProcess)

    override def create(toTokenize:InputStream):Tokenizer = ???
  }

  class SI(file:File, prog:(Int, String) => Boolean) extends BaseSentenceIterator {
    private[this] var in = open()
    private[this] val queue = mutable.Buffer[String]()

    private[this] var lines = 0

    override def finish():Unit = synchronized {
      in.close()
    }

    override def nextSentence():String = synchronized {
      if(hasNext) {
        val content = queue.remove(0)
        if(queue.isEmpty) fill()
        content
      } else throw new IllegalStateException()
    }

    override def hasNext:Boolean = synchronized {
      if(queue.nonEmpty) true else {
        fill()
        queue.nonEmpty
      }
    }

    override def reset():Unit = synchronized {
      in.close()
      in = open()
      queue.clear()
      lines = 0
      prog(0, "")
    }

    private[this] def open():BufferedReader = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(file)), "UTF-8"))

    private[this] def fill():Unit = synchronized {
      Option(in.readLine()).map(_.split("\t", 3)) match {
        case Some(Array(_, title, content)) =>
          val it = BreakIterator.getSentenceInstance
          it.setText(content)
          (it.first() +: Iterator.continually(it.next()).takeWhile(_ != BreakIterator.DONE).toList).sliding(2).foreach {
            case begin :: end :: Nil =>
              val raw = content.substring(begin, end).trim()
              val sentence = if(preProcessor == null) raw else preProcessor.preProcess(raw)
              if(sentence.nonEmpty) {
                queue.append(sentence)
              }
            case _ => throw new IllegalStateException()
          }
          lines += 1
          prog(lines, title)
          if(queue.isEmpty) {
            fill()
          }
        case None => ()
      }
    }
  }

  // def parse(text:String):Seq[Int] = {}

}
