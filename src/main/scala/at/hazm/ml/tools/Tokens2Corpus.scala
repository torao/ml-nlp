package at.hazm.ml.tools

import java.io.{BufferedReader, File, FileInputStream, InputStreamReader}
import java.util.zip.GZIPInputStream

import at.hazm.ml.nlp.Corpus

import scala.io.Source

object Tokens2Corpus extends App{

  val (src, dict) = args.toList match {
    case s :: d :: _ => (s, d)
    case _ =>
      System.err.println(s"USAGE: ${getClass.getName.dropRight(1)} [docid-terms.tsv.gz] [corpus]")
      System.exit(1)
      throw new Exception()
  }

  val corpus = new Corpus(new File(dict))
  val vocab = new corpus.Vocabulary()
  vocab.bulk{ db =>
    Source.fromInputStream(new GZIPInputStream(new FileInputStream(src)), "UTF-8").getLines().map(_.split("\t", 2)).foreach{
      case Array(_, terms) => terms.split("\\s+").foreach(db)
    }
  }

}
