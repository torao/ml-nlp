package at.hazm.ml.etl

/**
  * ETL のための何らかのデータを取り出すことのできる trait です。
  * このインスタンスはファイルやデータベース接続などのリソースを保持する事を想定しています。
  *
  * @tparam T このパイプから取り出すデータの型
  */
trait Pipe[T] extends Iterator[T] with AutoCloseable {

  /**
    * このパイプを初期状態に戻しもう一度最初のデータから取り出せるようにします。
    */
  def reset():Unit
}
