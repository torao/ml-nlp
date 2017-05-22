package at.hazm.core.io

import java.io._

class ProgressReader(_in:Reader, callback:(Long,Long)=>Unit) extends FilterReader(_in) {
  private[this] var position = 0L
  private[this] var line = 1L

  def this(file:File, callback:(Long,Long)=>Unit) = this(new FileReader(file), callback)

  override def read():Int = {
    val ch = super.read()
    if(ch >= 0){
      position += 1
      if(ch == '\n'){
        line += 1
      }
      callback(position, line)
    }
    ch
  }

  override def read(cbuf:Array[Char], off:Int, len:Int):Int = {
    val l = super.read(cbuf, off, len)
    if(l >= 0){
      position += l
      for(i <- off until (off + l)){
        if(cbuf(i) == '\n') line += 1
      }
      callback(position, line)
    }
    l
  }

  override def read(b:Array[Char]):Int = read(b, 0, b.length)
}
