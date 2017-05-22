package at.hazm.ml.nlp.pipeline

/**
  * 出力データのフィルタリングと変換の再利用可能な機能を実装します。出力に対するフィルタ PUSH 型の動作を行います。
  *
  * @tparam IN  出力データに変換する型
  * @tparam OUT 出力データ型
  */
trait ExhaustFilter[IN, OUT] extends Destination[IN] {

  /**
    * 指定された入力データを出力データに変換します。
    *
    * @param data 入力データ
    * @return 出力データ
    */
  def backward(data:IN):OUT

  override def write(data:IN):Unit = throw new NoSuchElementException("exhaust filter cannot write anything")

  override def close():Unit = ()

}

object ExhaustFilter {
  def apply[IN, OUT](f:(IN) => OUT):ExhaustFilter[IN, OUT] = (data:IN) => f(data)

}