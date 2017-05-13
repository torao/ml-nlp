package at.hazm.ml.nlp.pipeline

import java.io.{BufferedReader, File, PrintWriter}
import java.nio.charset.StandardCharsets

import at.hazm.ml.io.{openTextInput, openTextOutput}
import at.hazm.ml.nlp.pipeline.IntakePipe.Joint

import scala.collection.mutable

/**
  * データのフィルタリングと変換処理の再利用可能な機能を実装します。パイプはデータのコンシューマ側から PULL 型の動作を行います。
  *
  * @tparam IN  変換元の型
  * @tparam OUT 変換後の型
  */
trait IntakePipe[IN, OUT] extends Source[OUT] with Pipe[IN, OUT] {
  private[pipeline] var _src:Option[Source[IN]] = None

  private[pipeline] def joint(prev:Source[IN]):Unit = {
    assert(_src.isEmpty, s"$this is already joined another pipe")
    _src = Some(prev)
  }

  def |[T](next:IntakePipe[OUT, T]):IntakePipe[IN, T] = new Joint(this, next)

  /**
    * このパイプのソースを参照します。
    *
    * @return このパイプのソース
    */
  def src:Source[IN] = _src.get

  /**
    * このパイプから次のデータが取り出せるかを判定します。このメソッドが true を返した場合、[[next()]] は例外なしで (システム由来で
    * 予測が不可能な例外を除く) データを返す必要があります。
    *
    * @return
    */
  def hasNext:Boolean = src.hasNext

  /**
    * パイプから次のデータを取り出します。[[hasNext]] が false を返している状態でこのメソッドを呼び出すことはできません。
    *
    * @return 次のデータ
    * @throws NoSuchElementException [[hasNext]] が false を返しておりこれ以上データが存在しない場合
    */
  @throws[NoSuchElementException]
  def next():OUT = transform(src.next())

  /**
    * このパイプおよびその入力方向のソースをクローズしリソースを開放します。
    */
  def close():Unit = _src.foreach(_.close())

}

object IntakePipe {

  def apply[IN,OUT](f:(IN)=>OUT):IntakePipe[IN,OUT] = (data:IN) => f(data)

  private[pipeline] class Joint[IN, T, OUT](src:IntakePipe[IN, T], dest:IntakePipe[T, OUT]) extends IntakePipe[IN, OUT] {
    dest.joint(src)

    override private[pipeline] def joint(prev:Source[IN]) = {
      super.joint(prev)
      src.joint(prev)
    }

    override def hasNext:Boolean = dest.hasNext

    override def next():OUT = dest.next()

    override def transform(data:IN):OUT = throw new UnsupportedOperationException
  }

  /**
    * リスト化された入力値を単一の型に分解します。`Flatten[Seq[A],A]()` のように記述します。
    *
    * @tparam IN  変換元の型
    * @tparam OUT 変換後の型
    */
  case class Flatten[IN <: TraversableOnce[OUT], OUT]() extends IntakePipe[IN, OUT] {
    private[this] val cache = mutable.Buffer[OUT]()

    override def hasNext:Boolean = if(cache.nonEmpty) true else {
      if(src.hasNext) {
        cache.appendAll(src.next())
        true
      } else false
    }

    override def next():OUT = cache.remove(0)

    override def transform(data:IN):OUT = throw new UnsupportedOperationException
  }

  /**
    * 入力値をローカルファイルにキャッシュするパイプです。すでにファイルが存在する場合は入力側からの入力は行わずこのパイプを起点に
    * キャッシュされている内容を返します。
    *
    * @param file キャッシュファイル
    */
  case class Cache(file:File) extends IntakePipe[String, String] {
    private[this] var line:String = _
    private[this] val io:Either[BufferedReader, PrintWriter] = if(file.exists()) {
      val in = openTextInput(file, StandardCharsets.UTF_8)
      line = in.readLine()
      Left(in)
    } else {
      Right(openTextOutput(file, StandardCharsets.UTF_8))
    }

    override def hasNext:Boolean = io match {
      case Left(_) => line != null
      case Right(_) => src.hasNext
    }

    override def next():String = io match {
      case Left(in) =>
        val current = line
        line = in.readLine()
        current
      case Right(out) =>
        val current = src.next()
        out.println(current)
        out.flush()
        current
    }

    override def close():Unit = {
      io match {
        case Left(in) => in.close()
        case Right(out) => out.close()
      }
      super.close()
    }

    override def transform(data:String):String = throw new UnsupportedOperationException
  }

}