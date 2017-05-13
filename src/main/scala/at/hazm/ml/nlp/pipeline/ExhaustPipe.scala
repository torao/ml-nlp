package at.hazm.ml.nlp.pipeline

trait ExhaustPipe[IN, OUT] extends Destination[IN] with Pipe[IN, OUT] {
  private[pipeline] var _dest:Option[Destination[OUT]] = None

  /**
    * このパイプの宛先を参照します。
    *
    * @return このパイプのソース
    */
  def dest:Destination[OUT] = _dest.get

  override def write(data:IN):Unit = dest.write(transform(data))

  override def close():Unit = dest.close()

}

object ExhaustPipe {
  def apply[IN,OUT](f:(IN)=>OUT):ExhaustPipe[IN,OUT] = (data:IN) => f(data)

}