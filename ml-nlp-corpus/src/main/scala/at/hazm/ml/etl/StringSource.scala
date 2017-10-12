package at.hazm.ml.etl

/**
  * 指定された文字列を全データとして扱う Source です。
  *
  * @param text データの文字列
  */
class StringSource(text:String) extends Source[String] {

  private[this] var data:Option[String] = None
  reset()

  override def reset():Unit = data = Some(text)

  override def hasNext:Boolean = data.isDefined

  override def next():String = {
    val t = data.get
    data = None
    t
  }
}

