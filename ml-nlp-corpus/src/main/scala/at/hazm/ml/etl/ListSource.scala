package at.hazm.ml.etl

/**
  * 指定された値集合を順に参照できる Source です。
  *
  * @param values リストデータ
  */
class ListSource[T](values:List[T]) extends Source[T] {

  private[this] var i:Int = 0

  override def reset():Unit = i = 0

  override def hasNext:Boolean = i < values.length

  override def next():T = {
    val t = values(i)
    i += 1
    t
  }
}

