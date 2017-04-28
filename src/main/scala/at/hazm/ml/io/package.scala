package at.hazm.ml

import java.io._
import java.nio.charset.{Charset, StandardCharsets}
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import scala.language.reflectiveCalls

package object io {
  def using[T <: {def close() :Unit}, U](resource:T)(f:(T) => U):U = try {
    f(resource)
  } finally {
    resource.close()
  }
  def using[T1 <: {def close() :Unit}, T2 <: {def close() :Unit}, U](r1:T1, r2:T2)(f:(T1,T2) => U):U = using(r1){ x1 =>
    using(r2){ x2 =>
      f(x1, x2)
    }
  }

  def readBinary[T](file:File, bufferSize:Int = 0, callback:(Long,Long)=>Unit = null)(f:(InputStream)=>T):T = {
    val len = file.length()
    val fs = if(callback == null) new FileInputStream(file) else new ProgressInputStream(file, { pos:Long => callback(pos, len) })
    val is = if(file.getName.endsWith(".gz")) new GZIPInputStream(fs) else fs
    using(if(bufferSize == 0) is else new BufferedInputStream(is, bufferSize))(f)
  }

  def readText[T](file:File, charset:Charset, bufferSize:Int = 0, callback:(Long,Long)=>Unit = null)(f:(Reader)=>T):T = readBinary(file){ is =>
    val r = new InputStreamReader(is, charset)
    using(if(callback == null) r else new ProgressReader(r, { (pos:Long, line:Long) => callback(pos, line) }))(f)
  }

  def readLine[T](file:File)(f:(BufferedReader)=>T):T = {
    val fs = new FileInputStream(file)
    val is = if(file.getName.endsWith(".gz")) new GZIPInputStream(fs) else fs
    using(new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8)))(f)
  }

  def writeLine[T](file:File, append:Boolean = false)(f:(PrintWriter)=>T):T = {
    val fs = new FileOutputStream(file, append)
    val os = if(file.getName.endsWith(".gz")) new GZIPOutputStream(fs) else fs
    using(new PrintWriter(new OutputStreamWriter(os, StandardCharsets.UTF_8)))(f)
  }

}
