package at.hazm.ml.nlp.pipeline

/**
  * 入力から出力までのパイプラインを作成します。
  *
  * @param src  入力
  * @param dest 出力
  * @tparam INTAKE  入力型
  * @tparam EXHAUST 出力型
  */
class Pipeline[INTAKE, EXHAUST] private(src:Source[INTAKE], dest:Destination[EXHAUST]) extends Source[INTAKE] with Destination[EXHAUST] {

  /**
    * このパイプラインから次のデータが取り出されるかを確認します。
    *
    * @return パイプラインからデータを取り出せる場合 true
    */
  override def hasNext:Boolean = src.hasNext

  /**
    * このパイプラインから次のデータを取り出します。パイプラインに取り出せるデータが存在しない場合は例外が発生します。
    *
    * @return パイプラインから取り出したデータ
    */
  @throws[NoSuchElementException]
  override def next():INTAKE = src.next()

  /**
    * このパイプラインにデータを書き戻します。
    *
    * @param data 出力するデータ
    */
  override def write(data:EXHAUST):Unit = dest.write(data)

  /**
    * このパイプラインをリセットし読み込み位置を初期状態に戻します。
    */
  override def reset():Unit = src.reset()

  /**
    * このパイプラインをクローズします。
    */
  override def close():Unit = {
    src.close()
    dest.close()
  }
}

object Pipeline {

  class Builder[INTAKE, EXHAUST](src:Source[INTAKE], dest:Destination[EXHAUST]) {
    /**
      * ラムダで指定する場合は出力の型を明示する必要があります。
      * {{{
      *   builder.forward[Int] { i:String => i.toInt}
      * }}}
      * @param f
      * @tparam T
      * @return
      */
    def forward[T](f:IntakeFilter[INTAKE, T]):Builder[T, EXHAUST] = new Builder(src >> f, dest)
    def forward[T](f:(INTAKE)=>T):Builder[T, EXHAUST] = new Builder(src >> f, dest)

    def backward[T](f:ExhaustFilter[T, EXHAUST]):Builder[INTAKE, T] = new Builder(src, dest << f)
    def backward[T](f:(T)=>EXHAUST):Builder[INTAKE, T] = new Builder(src, dest << f)

    def build:Pipeline[INTAKE, EXHAUST] = new Pipeline(src, dest)
  }

}