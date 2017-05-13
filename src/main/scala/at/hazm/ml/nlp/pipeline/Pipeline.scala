package at.hazm.ml.nlp.pipeline

case class Pipeline[INTAKE, EXHAUST](src:Source[INTAKE], dest:Destination[EXHAUST]) extends Source[INTAKE] with Destination[EXHAUST] {
  override def hasNext:Boolean = src.hasNext

  override def next():INTAKE = src.next()

  override def write(data:EXHAUST):Unit = dest.write(data)

  override def close():Unit = {
    src.close()
    dest.close()
  }
}
