package at.hazm.core

import java.io._
import java.nio.charset.{Charset, StandardCharsets}
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import org.apache.commons.compress.compressors.bzip2.{BZip2CompressorInputStream, BZip2CompressorOutputStream}
import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.collection.mutable
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

  def readAllBytes(in:InputStream, bufferSize:Int = DefaultIOBufferSize):Array[Byte] = {
    val out = new ByteArrayOutputStream()
    copy(in, out, bufferSize)
    out.toByteArray
  }

  def copy(in:InputStream, out:OutputStream, bufferSize:Int = DefaultIOBufferSize):Unit = {
    @tailrec
    def _copy(in:InputStream, out:OutputStream, buffer:Array[Byte]):Unit = {
      val len = in.read(buffer)
      if(len >= 0) {
        out.write(buffer, 0, len)
        _copy(in, out, buffer)
      }
    }

    _copy(in, out, new Array[Byte](bufferSize))
  }

  /**
    * 指定されたファイルを読み出し用にオープンします。ファイル名が .gz または .bz2 を持つ場合は圧縮の解除を行った入力ストリームを構築
    * します。
    *
    * @param file       ファイル
    * @param bufferSize バッファサイズ (バイト数)
    * @param callback   読み出し済みの (バイト数, 行数) コールバック
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
    * @param callback   読み出し済みの (文字数, 行数) コールバック
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
    val out = new PrintWriter(new OutputStreamWriter(os, charset))
    val result = f(out)
    out.flush()
    result
  }


  object text {
    private[this] val logger = LoggerFactory.getLogger(getClass.getName.dropRight(1))

    trait CloseableIterator[T] extends Iterator[T] with AutoCloseable

    private[io] def parser(_in:Reader, fieldSeparator:Char):CloseableIterator[Seq[String]] = new CloseableIterator[Seq[String]]() {
      private[this] val in = new at.hazm.core.io.LineNumberReader(_in, 100)
      private[this] var cache:Option[Seq[String]] = None
      private[this] var closed = false

      override def close():Unit = {
        closed = true
        in.close()
      }

      override def hasNext:Boolean = cache match {
        case Some(_) => true
        case None =>
          cache = purge()
          cache.isDefined
      }

      override def next():Seq[String] = cache match {
        case Some(value) =>
          cache = None
          value
        case None =>
          purge().get
      }

      private[this] def eof:Boolean = if(closed) true else {
        val c = in.read()
        if(c < 0) true else {
          in.unread(c)
          false
        }
      }

      private[this] def eol:Boolean = if(closed) true else {
        val c1 = in.read()
        if(c1 < 0) true else if(c1 == '\n') true else if(c1 == '\r') {
          val c2 = in.read()
          if(c2 == '\n') true else {
            in.unread(c2)
            true
          }
        } else {
          in.unread(c1)
          false
        }
      }

      private[this] def purge():Option[Seq[String]] = {
        @tailrec
        def _readField(quoted:Boolean, buffer:StringBuilder):(String, Int) = if(eof) {
          (buffer.toString(), -1)
        } else if(!quoted && eol) {
          (buffer.toString(), '\n')
        } else (quoted, in.read()) match {
          case (true, eof) if eof < 0 =>
            logger.warn(f"eof detected while reading quoted field: $buffer")
            (buffer.toString(), eof)
          case (true, '\"') =>
            if(eof) {
              logger.warn(f"eof detected while reading quoted field: $buffer")
              (buffer.toString(), -1)
            } else if(eol) {
              (buffer.toString(), '\n')
            } else in.read() match {
              case '\"' =>
                buffer.append("\"")
                _readField(quoted, buffer)
              case eof if eof == fieldSeparator =>
                (buffer.toString(), eof)
              case ch2 =>
                logger.warn(s"[${in.line + 1}:${in.column + 1}]: double-quote expect: $buffer${ch2.toChar}")
                buffer.append("\"")
                buffer.append(ch2.toChar)
                _readField(quoted = false, buffer)
            }
          case (true, c) =>
            buffer.append(c.toChar)
            _readField(quoted, buffer)
          case (false, eof) if eof < 0 || eof == fieldSeparator =>
            (buffer.toString(), eof)
          case (false, '\"') if buffer.isEmpty =>
            _readField(quoted = true, buffer)
          case (false, c) =>
            buffer.append(c.toChar)
            _readField(quoted, buffer)
        }

        def _read(row:mutable.Buffer[String]):Option[Seq[String]] = {
          val (field, ch) = _readField(quoted = false, new StringBuilder())
          row.append(field)
          if(ch == fieldSeparator) {
            _read(row)
          } else if(ch == '\n') {
            Some(row)
          } else if(ch < 0) {
            if(row.size == 1 && field.isEmpty) None else Some(row)
          } else throw new IllegalStateException(ch.toString)
        }

        _read(mutable.Buffer[String]())
      }
    }
  }

  object csv {
    def quote(value:Any):String = value match {
      case text:String => "\"" + text.replace("\"", "\"\"") + "\""
      case i => i.toString
    }

    def parse(in:Reader):text.CloseableIterator[Seq[String]] = text.parser(in, ',')
  }

  object tsv {
    def parse(in:Reader):text.CloseableIterator[Seq[String]] = text.parser(in, '\t')
  }

}
