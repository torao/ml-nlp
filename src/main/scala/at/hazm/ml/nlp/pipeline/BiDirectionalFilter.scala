package at.hazm.ml.nlp.pipeline

trait BiDirectionalFilter[IN, OUT] extends IntakeFilter[IN, OUT] with ExhaustFilter[OUT, IN]

object BiDirectionalFilter {

  private[pipeline] class IOPipe[IN, OUT](f:(IN) => OUT, g:(OUT) => IN) extends BiDirectionalFilter[IN, OUT] {
    override def forward(data:IN):OUT = f(data)

    override def backward(data:OUT):IN = g(data)
  }

  /**
    * 指定されたラムダを変換処理に使用する入出力パイプを構築します。
    *
    * @param f 変換処理
    * @tparam IN  変換元の型
    * @tparam OUT 返還後の型
    * @return パイプ
    */
  def apply[IN, OUT](f:(IN) => OUT, g:(OUT) => IN):BiDirectionalFilter[IN, OUT] = new IOPipe(f, g)

  /**
    * 入力を無変換で出力する入出力パイプです。
    *
    * @tparam T 入出力の型
    */
  case class Noop[T]() extends IOPipe[T, T](identity, identity)

}