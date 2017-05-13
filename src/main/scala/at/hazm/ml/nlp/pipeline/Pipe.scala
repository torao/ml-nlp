package at.hazm.ml.nlp.pipeline

trait Pipe[IN,OUT] {

  /**
    * 指定された入力データを出力データに変換します。
    *
    * @param data 入力データ
    * @return 出力データ
    */
  def transform(data:IN):OUT

}

object Pipe {
  private[pipeline] class IOPipe[IN,OUT](f:(IN)=>OUT) extends IntakePipe[IN,OUT] with ExhaustPipe[IN,OUT] {
    override def transform(data:IN):OUT = f(data)
  }

  /**
    * 指定されたラムダを変換処理に使用する入出力パイプを構築します。
    *
    * @param f 変換処理
    * @tparam IN  変換元の型
    * @tparam OUT 返還後の型
    * @return パイプ
    */
  def apply[IN, OUT](f:(IN) => OUT):IntakePipe[IN,OUT] with ExhaustPipe[IN, OUT] = new IOPipe(f)

  /**
    * 入力を無変換で出力する入出力パイプです。
    *
    * @tparam T 入出力の型
    */
  case class Noop[T]() extends IOPipe[T, T](identity)

}