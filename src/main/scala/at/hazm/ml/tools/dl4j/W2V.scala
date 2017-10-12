package at.hazm.ml.tools.dl4j

import java.io._

import at.hazm.core.db.{BlobStore, LocalDB}
import at.hazm.ml.nlp.Corpus
import at.hazm.ml.nlp.word2vec.Word2Vec

object W2V extends App {

  val corpus = new Corpus(new LocalDB(new File(args(0))), "wikipedia")
  val fs = new BlobStore(new LocalDB(new File(args(1))), "word2vec")

  val (src, dest) = args.toList match {
    case s :: d :: _ => (new File(s), new File(d))
    case _ =>
      System.err.println(s"USAGE: ${getClass.getName.dropRight(1)} [corpus] [output]")
      System.exit(1)
      throw new Exception()
  }

  val model = Word2Vec.create(fs.Entry("default"), corpus)

}
