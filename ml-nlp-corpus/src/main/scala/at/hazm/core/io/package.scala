package at.hazm.core

import java.io._
import java.nio.charset.{Charset, StandardCharsets}
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import org.apache.commons.compress.compressors.bzip2.{BZip2CompressorInputStream, BZip2CompressorOutputStream}

import scala.annotation.tailrec
import scala.language.reflectiveCalls

package object io {
  type CLOSEABLE = {def close():Unit}

  val DefaultIOBufferSize:Int = 4 * 1024

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

  def readAllChars(in:Reader, bufferSize:Int = DefaultIOBufferSize):String = {
    @tailrec
    def _read(in:Reader, buf:Array[Char], buffer:StringBuilder):String = {
      val len = in.read(buf)
      if(len < 0) buffer.toString() else {
        buffer.appendAll(buf, 0, len)
        _read(in, buf, buffer)
      }
    }

    _read(in, new Array[Char](bufferSize), new StringBuilder(bufferSize))
  }

  /**
    * 指定されたファイルを読み出し用にオープンします。ファイル名が .gz または .bz2 を持つ場合は圧縮の解除を行った入力ストリームを構築
    * します。
    *
    * @param file       ファイル
    * @param bufferSize バッファサイズ (バイト数)
    * @return 入力ストリーム
    */
  def openBinaryInput(file:File, bufferSize:Int = DefaultIOBufferSize, callback:(Long, Long) => Unit = null):InputStream = {
    val len = file.length()
    val fs = if(callback == null) new FileInputStream(file) else new ProgressInputStream(file, { pos:Long => callback(pos, len) })
    val is = if(file.getName.endsWith(".gz")) {
      new GZIPInputStream(fs)
    } else if(file.getName.endsWith(".bz2")) {
      new BZip2CompressorInputStream(fs)
    } else fs
    if(bufferSize == 0) is else new BufferedInputStream(is, bufferSize)
  }

  /**
    * 指定されたファイルを読み出し用にオープンします。ファイル名が .gz または .bz2 を持つ場合は圧縮の解除を行った入力ストリームを構築
    * します。
    *
    * @param file       ファイル
    * @param charset    文字セット
    * @param bufferSize バッファサイズ (バイト数)
    * @return 入力ストリーム
    */
  def openTextInput(file:File, charset:Charset = StandardCharsets.UTF_8, bufferSize:Int = DefaultIOBufferSize, callback:(Long, Long) => Unit = null):BufferedReader = {
    val is = openBinaryInput(file, bufferSize)
    val r = new InputStreamReader(is, charset)
    val in = if(callback == null) r else new ProgressReader(r, { (pos:Long, line:Long) => callback(pos, line) })
    new BufferedReader(in)
  }

  /**
    * 指定されたファイルを書き込み用にオープンします。ファイル名が .gz または .bz2 を持つ場合は圧縮を行う出力ストリームを構築します。
    *
    * @param file       ファイル
    * @param bufferSize バッファサイズ (バイト数)
    * @return 入力ストリーム
    */
  def openBinaryOutput(file:File, bufferSize:Int = DefaultIOBufferSize):OutputStream = {
    val fs = new FileOutputStream(file)
    val os = if(file.getName.endsWith(".gz")) {
      new GZIPOutputStream(fs)
    } else if(file.getName.endsWith(".bz2")) {
      new BZip2CompressorOutputStream(fs)
    } else fs
    if(bufferSize == 0) os else new BufferedOutputStream(os, bufferSize)
  }

  /**
    * 指定されたファイルを書き込み用にオープンします。ファイル名が .gz または .bz2 を持つ場合は圧縮を行う出力ストリームを構築します。
    *
    * @param file       ファイル
    * @param charset    出力文字セット
    * @param bufferSize バッファサイズ (バイト数)
    * @return 入力ストリーム
    */
  def openTextOutput(file:File, charset:Charset = StandardCharsets.UTF_8, bufferSize:Int = DefaultIOBufferSize):PrintWriter = {
    val os = openBinaryOutput(file, bufferSize)
    val out = new OutputStreamWriter(os, charset)
    new PrintWriter(out)
  }

  def readBinary[T](file:File, bufferSize:Int = 0, callback:(Long, Long) => Unit = null)(f:(InputStream) => T):T = {
    using(openBinaryInput(file, bufferSize, callback))(f)
  }

  def readText[T](file:File, charset:Charset = StandardCharsets.UTF_8, bufferSize:Int = 0, callback:(Long, Long) => Unit = null)(f:(BufferedReader) => T):T = {
    f(openTextInput(file, charset, bufferSize, callback))
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
