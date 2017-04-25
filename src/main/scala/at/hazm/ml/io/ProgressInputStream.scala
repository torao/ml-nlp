package at.hazm.ml.io

import java.io.{File, FileInputStream, FilterInputStream, InputStream}

class ProgressInputStream(_in:InputStream, length:Long, callback:(Long,Long)=>Unit) extends FilterInputStream(_in) {
  def this(file:File, callback:(Long,Long)=>Unit) = this(new FileInputStream(file), file.length(), callback)

}
