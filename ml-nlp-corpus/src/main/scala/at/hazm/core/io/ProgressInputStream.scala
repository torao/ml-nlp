package at.hazm.core.io

import java.io.{File, FileInputStream, FilterInputStream, InputStream}

/**
  * 入力ストリームから読み込んだバイト数をコールバックするストリームです。巨大なファイルからの入力を処理する場合などの進捗を表示するためなど
  * に使用します。
  *
  * @param _in      下層の入力ストリーム
  * @param callback 読み出したバイト数のコールバック
  */
class ProgressInputStream(_in:InputStream, callback:(Long) => Unit) extends FilterInputStream(_in) {
  private[this] var position = 0L

  def this(file:File, callback:(Long) => Unit) = this(new FileInputStream(file), callback)

  override def read():Int = {
    position += 1
    callback(position)
    super.read()
  }

  override def read(b:Array[Byte], off:Int, len:Int):Int = {
    val l = super.read(b, off, len)
    position += l
    callback(position)
    l
  }

  override def read(b:Array[Byte]):Int = read(b, 0, b.length)
}
