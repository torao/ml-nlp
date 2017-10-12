package at.hazm.ml.etl

import java.io._

/**
  * 入力ストリームからテキストを参照する Source です。一度の呼び出しで取り出せる文字列の長さは不確定です。
  *
  * @param bufferSize 読み込みバッファのサイズ
  */
abstract class StreamSource(bufferSize:Int) extends Source[String] {

  /** 読み込みバッファ */
  private[this] val buffer = new Array[Char](bufferSize)

  /** 入力ストリーム */
  private[this] var in:PushbackReader = _

  reset()

  /**
    * このインスタンスが定義する入力ストリームをオープンして返します。入力ストリームのクローズはスーパークラスが行います。
    */
  protected def reopen():Reader

  override def hasNext:Boolean = {
    val ch = in.read()
    if(ch < 0) false else {
      in.unread(ch)
      true
    }
  }

  override def next():String = {
    val len = in.read(buffer)
    if(len < 0) {
      throw new EOFException()
    }
    new String(buffer, 0, len)
  }

  override def reset():Unit = {
    close()
    in = new PushbackReader(reopen())
  }

  override def close():Unit = {
    Option(in).foreach(_.close())
  }

}
