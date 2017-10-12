package at.hazm.ml.etl

/**
  * パイプラインとして結合されたソースです。
  *
  * @param source データソース
  * @param pipes  中間処理
  * @tparam T このソースから取り出すデータの型
  */
private[etl] class Pipeline[T](source:Source[_], pipes:List[Transform[_, _]]) extends Source[T] {

  // 全てのソースを連結する。
  pipes.headOption.foreach(_.source_=(source))
  pipes.sliding(2).foreach {
    case Seq(a, b) => b.source_=(a)
    case _ => None
  }

  /** 最末尾のソース */
  private[this] val last = pipes.lastOption.getOrElse(source).asInstanceOf[Source[T]]

  override def hasNext:Boolean = last.hasNext

  override def next():T = last.next()

  /**
    * このパイプラインが保持する全てのパイプとソースをリセットします。
    */
  override def reset():Unit = {
    pipes.reverse.foreach(_.reset())
    source.reset()
  }

  /**
    * このパイプラインが保持する全てのパイプとソースをクローズします。
    */
  override def close():Unit = {
    pipes.reverse.foreach(_.close())
    source.close()
  }

  /**
    * 指定されたパイプをこのパイプラインに接続します。このパイプラインの内容は新しいパイプラインに引き継がれます。
    *
    * @param pipe このソースに結合するパイプ
    * @tparam U 新しく生成されるソースから取り出せるデータ型
    * @return 新しいソース
    */
  override def append[U](pipe:Transform[T, U]):Source[U] = new Pipeline(source, pipes :+ pipe)
}
