package at.hazm.ml.etl

/**
  * ETL のための中間処理です。
  *
  * @tparam IN  下層のソースから取り出せるデータの型
  * @tparam OUT このソースから取り出せるデータの型
  */
trait Transform[IN, OUT] extends Source[OUT] {

  /** 下層のソース */
  private[this] var _source:Source[_] = _

  /**
    * サブクラスから下層のソースを参照します。
    */
  protected def source:Source[IN] = _source.asInstanceOf[Source[IN]]

  /**
    * このインスタンスに下層のソースを設定します。
    */
  private[etl] def source_=(src:Source[_]):Unit = _source = src
}
