package at.hazm.ml

import java.io._
import java.nio.{ByteBuffer, CharBuffer}
import java.nio.channels.FileChannel
import java.nio.charset.Charset
import java.nio.file.StandardOpenOption
import java.util.zip.GZIPInputStream

import at.hazm.ml.io.{readBinary, using}

import scala.annotation.tailrec
import scala.io.Source

package object tools {

  def countLines(file:File):Int = if(file.getName.endsWith(".gz")) {
    System.err.print(s"counting lines [${file.getName}] ")
    System.err.flush()
    var period = 0
    val count = readBinary(file, 512 * 1024, { (cur, max) =>
      if(period < (cur * 10) / max){
        System.err.print(period)
        System.err.flush()
        period += 1
      }
    }){ in =>
      @tailrec
      def _read(buf:Array[Byte], count:Int):Int = {
        val len = in.read(buf)
        if(len < 0) count
        else _read(buf, count + buf.take(len).count(_=='\n'))
      }
      _read(new Array[Byte](1024), 0)
    }
    System.err.println(": DONE")
    System.err.flush()
    count
  } else using(FileChannel.open(file.toPath, StandardOpenOption.READ)){ fc =>
    var line = 0
    val buf = ByteBuffer.allocate(512 * 1024)
    while(fc.position() < fc.size()){
      val len = fc.read(buf)
      buf.flip()
      for(i <- 0 until len){
        if(buf.get(i) == '\n') line += 1
      }
    }
    line
  }

  def progress[T](prompt:String, max:Int, interval:Long = 60 * 1000L)(f:((Int,String)=>Boolean)=>T):T = {
    def time(lt:Long):String = {
      val (d, h, m, s) = (lt / 24 / 60 / 60, lt / 60 / 60 % 24, lt / 60 % 60, lt % 60)
      if(d == 0) f"$h%02d:$m%02d:$s%02d" else f"$d%dd $h%02d:$m%02d"
    }
    var t0 = System.currentTimeMillis()
    var step = 0
    f({ (cur,label) =>
      val t = System.currentTimeMillis()
      if(cur == 0){
        System.err.println(f"$prompt: $cur%,d / $max%,d (${cur.toDouble/max*100}%.1f%%) ${if(step>0) "restart" else "start"}")
        t0 = System.currentTimeMillis()
        step = 0
      } else if(t - (t0 + interval * step) > interval){
        val lt = ((t - t0) * max.toDouble / cur - (t - t0)).toInt / 1000
        val left = time(lt)
        System.err.println(f"$prompt: $cur%,d / $max%,d (${cur.toDouble/max*100}%.1f%%) $left%s: $label%s")
        while(t0 + interval * step <= t) step += 1
      } else if(cur == max){
        val spent = time(t - t0)
        System.err.println(f"$prompt: $cur%,d / $max%,d (${cur.toDouble/max*100}%.1f%%) finish in $spent%s")
      }
      true
    })
  }

  def progress(file:File, charset:Charset)(f:(String)=>Unit):Unit = progress(file.getName, countLines(file)){ prog =>
    readBinary(file){ is =>
      Source.fromInputStream(is, charset.name()).getLines().zipWithIndex.foreach{ case (line, i) =>
        f(line)
        prog(i + 1, line.take(25))
      }
    }
  }

  class NewLineCallbackReader(in:Reader, newLineListener:(Int)=>Unit) extends FilterReader(in) {
    private[this] var line = 1
    override def read():Int = {
      val ch = super.read()
      if(ch == '\n'){
        line += 1
        newLineListener(line)
      }
      ch
    }

    override def read(cbuf:Array[Char]):Int = read(cbuf, 0, cbuf.length)

    override def read(cbuf:Array[Char], off:Int, len:Int):Int = {
      val length = super.read(cbuf, off, len)
      if(length >= 0){
        for(i <- off until (off + length)){
          if(cbuf(i) == '\n'){
            line += 1
            newLineListener(line)
          }
        }
      }
      length
    }

    override def read(target:CharBuffer):Int = super.read(target)
  }
}
