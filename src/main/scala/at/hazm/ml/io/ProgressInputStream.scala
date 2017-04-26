package at.hazm.ml.io

import java.io.{File, FileInputStream, FilterInputStream, InputStream}

class ProgressInputStream(_in:InputStream, length:Long, callback:(Long,Long)=>Unit) extends FilterInputStream(_in) {
  private[this] var position = 0

  def this(file:File, callback:(Long,Long)=>Unit) = this(new FileInputStream(file), file.length(), callback)

  override def read():Int = {
    position += 1
    callback(position, length)
    super.read()
  }

  override def read(b:Array[Byte], off:Int, len:Int):Int = {
    val l = super.read(b, off, len)
    position += l
    callback(position, length)
    l
  }

  override def read(b:Array[Byte]):Int = read(b, 0, b.length)
}
