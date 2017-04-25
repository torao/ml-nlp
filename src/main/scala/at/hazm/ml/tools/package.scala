package at.hazm.ml

import java.io.{BufferedInputStream, BufferedReader, File, FileInputStream}
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.StandardOpenOption
import java.util.zip.GZIPInputStream

import at.hazm.ml.io.using

import scala.io.Source

package object tools {

  def countLines(file:File):Int = {
    /*if(file.getName.endsWith(".gz")) {
      using(new GZIPInputStream(new BufferedInputStream(new FileInputStream(file)))){ in =>
        Source.fromInputStream(in).getLines().size
      }
    } else */using(FileChannel.open(file.toPath, StandardOpenOption.READ)){ fc =>
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
  }

  def progress[T](prompt:String, max:Int, interval:Long = 60 * 1000L)(f:((Int,String)=>Boolean)=>T):T = {
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
        val (d, h, m, s) = (lt / 24 / 60 / 60, lt / 60 / 60 % 24, lt / 60 % 60, lt % 60)
        val left = if(d == 0) f"$h%02d:$m%02d:$s%02d" else f"$d%dd $h%02d:$m%02d"
        System.err.println(f"$prompt: $cur%,d / $max%,d (${cur.toDouble/max*100}%.1f%%) $left%s: $label%s")
        while(t0 + interval * step <= t) step += 1
      } else if(cur == max){
        System.err.println(f"$prompt: $cur%,d / $max%,d (${cur.toDouble/max*100}%.1f%%) finish")
      }
      true
    })
  }
}
