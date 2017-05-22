package at.hazm.ml.nlp.pipeline

import java.io.{BufferedReader, File, PrintWriter}
import java.nio.charset.StandardCharsets

import at.hazm.core.io.{openTextInput, openTextOutput}

import scala.collection.mutable

/**
  * 入力データのフィルタリングと変換の再利用可能な機能を実装します。入力に対するフィルタ PULL 型の動作を行います。
  *
  * Iterator のメソッドはこのインスタンスがメモリや二次記憶装置にキャッシュしているデータや保留されているデータを取得するために使用されます。
  * このようなキャッシュやバッファリングの動作を実装する場合、`forward()` の呼び出しによって前段から受け渡されるデータに対してそれらの
  * 動作を実装することができます。
  *
  * @tparam IN  変換元の型
  * @tparam OUT 変換後の型
  */
trait IntakeFilter[IN, OUT] extends Source[OUT] {

  /**
    * 指定された入力データを出力データに変換します。
    *
    * @param data 入力データ
    * @return 出力データ
    */
  def forward(data:IN):OUT

  /**
    * このパイプから次のデータが取り出せるかを判定します。このメソッドが true を返した場合、[[next()]] は例外なしで (システム由来で
    * 予測が不可能な例外を除く) データを返す必要があります。
    *
    * @return
    */
  def hasNext:Boolean = false

  /**
    * パイプから次のデータを取り出します。[[hasNext]] が false を返している状態でこのメソッドを呼び出すことはできません。
    *
    * @return 次のデータ
    * @throws NoSuchElementException [[hasNext]] が false を返しておりこれ以上データが存在しない場合
    */
  @throws[NoSuchElementException]
  def next():OUT = throw new NoSuchElementException("intake filter has not elements")

  /**
    * このパイプを初期状態に戻し読み込み位置をリセットします。
    */
  def reset():Unit = ()

  /**
    * このパイプおよびその入力方向のソースをクローズしリソースを開放します。
    */
  def close():Unit = ()

}

object IntakeFilter {

  def apply[IN, OUT](f:(IN) => OUT):IntakeFilter[IN, OUT] = (data:IN) => f(data)

  /**
    * リスト化された入力値を単一の型に分解します。`Flatten[Seq[A],A]()` のように記述します。
    *
    * @tparam IN  変換元の型
    * @tparam OUT 変換後の型
    */
  case class Flatten[IN <: TraversableOnce[OUT], OUT]() extends IntakeFilter[IN, OUT] {
    private[this] val cache = mutable.Buffer[OUT]()

    override def hasNext:Boolean = cache.nonEmpty

    override def next():OUT = cache.remove(0)

    override def forward(data:IN):OUT = {
      cache.appendAll(data)
      next()
    }

    override def reset():Unit = cache.clear()
  }

  /**
    * 入力値をローカルファイルにキャッシュするパイプです。すでにファイルが存在する場合は入力側からの入力は行わずこのパイプを起点に
    * キャッシュされている内容を返します。
    *
    * @param file キャッシュファイル
    */
  case class Cache[T](file:File, marshal:(T)=>String, unmarshal:(String)=>T) extends IntakeFilter[T, T] {
    private[this] var line:String = _
    private[this] var io = open()

    private[this] def open():Either[BufferedReader, PrintWriter] = if(file.exists()) {
      val in = openTextInput(file, StandardCharsets.UTF_8)
      line = in.readLine()
      Left(in)
    } else {
      Right(openTextOutput(file, StandardCharsets.UTF_8))
    }

    override def hasNext:Boolean = io match {
      case Left(_) => line != null
      case Right(_) => false
    }

    override def next():T = io match {
      case Left(in) =>
        val current = line
        line = in.readLine()
        unmarshal(current)
      case Right(_) => throw new NoSuchElementException("")
    }

    override def reset():Unit = {
      io match {
        case Left(in) => in.close()
        case Right(out) => out.close()
      }
      io = open()
    }

    override def close():Unit = {
      io match {
        case Left(in) => in.close()
        case Right(out) => out.close()
      }
      super.close()
    }

    override def forward(data:T):T = io match {
      case Left(_) => throw new NoSuchElementException("")
      case Right(out) =>
        out.println(marshal(data))
        out.flush()
        data
    }
  }

}