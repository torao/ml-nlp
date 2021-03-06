package at.hazm.ml.etl

import at.hazm.ml.etl.Source.{Filter, Injection}

import scala.annotation.tailrec
import scala.collection.mutable

/**
  * ETL のための何らかのデータを取り出すことのできる trait です。
  * このインスタンスはファイルやデータベース接続などのリソースを保持する事を想定しています。
  *
  * @tparam T このソースから取り出すデータの型
  */
trait Source[T] extends Pipe[T] {

  /**
    * このソースが使用していたリソースを開放します。
    */
  override def close():Unit = None

  /**
    * このソースに指定されたパイプを合成し新しいソースを形成します。パイプを使用することでデータが保存されている
    * 物理ストレージとそのフォーマット変換やフィルタ処理を分離し部品化することができます。
    * {{{
    *   FileSource("foo.txt") :> LineText() :> TSV()
    * }}}
    *
    * @param pipe このソースに結合するパイプ
    * @tparam U 新しく生成されるソースから取り出せるデータ型
    * @return 新しいソース
    */
  def append[U](pipe:Transform[T, U]):Source[U] = new Pipeline(this, List(pipe))

  /** append() のエイリアスです。 */
  def :>[U](pipe:Transform[T, U]):Source[U] = append(pipe)

  /**
    * 指定された関数を Transform として連結します。
    *
    * @param f このソースに結合する関数
    * @tparam U 新しく生成されるソースから取り出せるデータ型
    * @return 新しいソース
    */
  def append[U](f:(T) => U):Source[U] = append(new Injection(f))

  /** append() のエイリアスです。 */
  def :>[U](f:(T) => U):Source[U] = append(f)

  /**
    * 指定されたフィルタ関数を連結します。
    *
    * @param f 評価関数
    * @return 評価関数によって選別されたソース
    */
  def appendFilter(f:(T) => Boolean):Source[T] = append(new Filter(f))

  /** appendFilter() のエイリアスです。 */
  def :|(f:(T) => Boolean):Source[T] = appendFilter(f)


}

object Source {

  def sequence[T](sources:Source[T]*):Source[T] = new SequenceSource[T](sources)

  /**
    * 指定されたソースを結合して連続したデータを参照します。
    *
    * @param sources 連結するソース
    */
  private class SequenceSource[T](sources:Seq[Source[T]]) extends Source[T] {
    private[this] val queue = mutable.Buffer[Source[T]](sources:_*)

    override def hasNext:Boolean = queue.exists(_.hasNext)

    override def next():T = {
      val src = queue.head
      if(src.hasNext) {
        src.next()
      } else {
        queue.remove(0)
        this.next()
      }
    }

    override def reset():Unit = {
      sources.foreach(_.reset())
      queue.clear()
      queue.appendAll(sources)
    }

    override def close():Unit = sources.foreach(_.close())
  }

  /**
    * 指定された関数を使用して 1:1 のデータ変換を行う Transform を作成します。
    *
    * @param f 変換処理
    * @tparam IN  入力データ型
    * @tparam OUT 出力データ型
    */
  private[Source] class Injection[IN, OUT](f:(IN) => OUT) extends Transform[IN, OUT] {

    override final def hasNext:Boolean = source.hasNext

    override final def next():OUT = transform(source.next())

    override def reset():Unit = None

    override def close():Unit = None

    def transform(data:IN):OUT = f(data)
  }

  /**
    * 指定された評価関数を使用してデータのフィルタリングを行う Transform を作成します。
    *
    * @param f 評価関数
    * @tparam T 入出力データ型
    */
  private[Source] class Filter[T](f:(T) => Boolean) extends Transform[T, T] {
    private[this] var cache:Option[T] = None

    override final def hasNext:Boolean = {
      prefetch()
      cache.isDefined
    }

    override final def next():T = {
      val value = cache.get
      cache = None
      value
    }

    override def reset():Unit = cache = None

    override def close():Unit = cache = None

    @tailrec
    private[this] def prefetch():Unit = if(source.hasNext) {
      val value = source.next()
      if(f(value)) cache = Some(value) else prefetch()
    } else {
      cache = None
    }
  }

}
