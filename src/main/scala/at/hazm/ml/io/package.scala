package at.hazm.ml

import java.io._
import java.nio.charset.{Charset, StandardCharsets}
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import org.apache.commons.compress.compressors.bzip2.{BZip2CompressorInputStream, BZip2CompressorOutputStream}

import scala.language.reflectiveCalls

package object io {
  type CLOSEABLE = {def close():Unit}

  def using[T <: CLOSEABLE, U](resource:T)(f:(T) => U):U = try {
    f(resource)
  } finally {
    resource.close()
  }

  def using[T1 <: CLOSEABLE, T2 <: CLOSEABLE, U](r1:T1, r2:T2)(f:(T1, T2) => U):U = using(r1) { x1 =>
    using(r2) { x2 =>
      f(x1, x2)
    }
  }

  def readBinary[T](file:File, bufferSize:Int = 0, callback:(Long, Long) => Unit = null)(f:(InputStream) => T):T = {
    val len = file.length()
    val fs = if(callback == null) new FileInputStream(file) else new ProgressInputStream(file, { pos:Long => callback(pos, len) })
    val is = if(file.getName.endsWith(".gz")) {
      new GZIPInputStream(fs)
    } else if(file.getName.endsWith(".bz2")) {
      new BZip2CompressorInputStream(fs)
    } else fs
    using(if(bufferSize == 0) is else new BufferedInputStream(is, bufferSize))(f)
  }

  def readText[T](file:File, charset:Charset = StandardCharsets.UTF_8, bufferSize:Int = 0, callback:(Long, Long) => Unit = null)(f:(BufferedReader) => T):T = readBinary(file) { is =>
    val r = new InputStreamReader(is, charset)
    val in = if(callback == null) r else new ProgressReader(r, { (pos:Long, line:Long) => callback(pos, line) })
    f(new BufferedReader(in))
  }

  def writeBinary[T](file:File, append:Boolean = false, bufferSize:Int = 0)(f:(OutputStream) => T):T = {
    val fs = new FileOutputStream(file, append)
    val os = if(file.getName.endsWith(".gz")) {
      new GZIPOutputStream(fs)
    } else if(file.getName.endsWith(".bz2")) {
      new BZip2CompressorOutputStream(fs)
    } else fs
    using(if(bufferSize == 0) os else new BufferedOutputStream(os, bufferSize))(f)
  }

  def writeText[T](file:File, append:Boolean = false, charset:Charset = StandardCharsets.UTF_8, bufferSize:Int = 0)(f:(PrintWriter) => T):T = writeBinary(file, append, bufferSize) { os =>
    f(new PrintWriter(new OutputStreamWriter(os, charset)))
  }

}
