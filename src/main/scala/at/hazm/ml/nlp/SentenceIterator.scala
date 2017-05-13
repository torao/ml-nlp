package at.hazm.ml.nlp

trait SentenceIterator extends Iterator[String] with AutoCloseable {
  def reset():Unit
}
