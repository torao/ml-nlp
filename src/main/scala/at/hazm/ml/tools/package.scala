package at.hazm.ml

import java.io._
import java.nio.channels.FileChannel
import java.nio.charset.Charset
import java.nio.file.StandardOpenOption
import java.nio.{ByteBuffer, CharBuffer}
import java.sql.Timestamp

import at.hazm.ml.io.Database._
import at.hazm.ml.io.{Database, readBinary, using}
import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.io.Source

package object tools {
  private[this] val logger = LoggerFactory.getLogger("at.hazm.ml.tools")

  def countLines(file:File, cacheDB:Database = null):Int = Option(cacheDB).flatMap { db =>
    // 行数が保存されていてファイルに変更がなければそれを返す
    db.trx { con =>
      con.createTable(s"file_info(name text not null primary key, length bigint not null, last_modified timestamp not null, lines integer not null)")
      con.headOption("select lines from file_info where name=? and length=? and last_modified=?",
        file.getCanonicalPath, file.length(), new Timestamp(file.lastModified()))(_.getInt(1))
    }
  }.getOrElse {
    val lines = if(file.getName.endsWith(".gz")) {
      System.err.print(s"counting lines [${file.getName}] ")
      System.err.flush()
      var period = 0
      val count = readBinary(file, 512 * 1024, { (cur, max) =>
        if(period < (cur * 10) / max) {
          System.err.print(period)
          System.err.flush()
          period += 1
        }
      }) { in =>
        @tailrec
        def _read(buf:Array[Byte], count:Int):Int = {
          val len = in.read(buf)
          if(len < 0) count
          else _read(buf, count + buf.take(len).count(_ == '\n'))
        }

        _read(new Array[Byte](1024), 0)
      }
      logger.info(": DONE")
      System.err.flush()
      count
    } else using(FileChannel.open(file.toPath, StandardOpenOption.READ)) { fc =>
      var line = 0
      val buf = ByteBuffer.allocate(512 * 1024)
      while(fc.position() < fc.size()) {
        val len = fc.read(buf)
        buf.flip()
        for(i <- 0 until len) {
          if(buf.get(i) == '\n') line += 1
        }
      }
      line
    }

    // 行数を保存
    Option(cacheDB).foreach { db =>
      db.trx { con =>
        con.exec("insert into file_info(name, length, last_modified, lines) values(?, ?, ?, ?)",
          file.getCanonicalPath, file.length(), new Timestamp(file.lastModified()), lines
        )
      }
    }

    lines
  }

  def progress[T](prompt:String, max:Int, interval:Long = 60 * 1000L)(f:((Int, String) => Boolean) => T):T = {
    def time(lt:Long):String = {
      val (d, h, m, s) = (lt / 24 / 60 / 60, lt / 60 / 60 % 24, lt / 60 % 60, lt % 60)
      if(d == 0) f"$h%02d:$m%02d:$s%02d" else f"$d%dd $h%02d:$m%02d"
    }

    var t0 = System.currentTimeMillis()
    var step = 0
    f({ (cur, label) =>
      val t = System.currentTimeMillis()
      if(cur == 0) {
        logger.info(f"$prompt: $cur%,d / $max%,d (${cur.toDouble / max * 100}%.1f%%) ${if(step > 0) "RESTART" else "START"}")
        t0 = System.currentTimeMillis()
        step = 0
      } else if(t - (t0 + interval * step) > interval) {
        val lt = ((t - t0) * max.toDouble / cur - (t - t0)).toLong / 1000L
        val left = time(lt)
        logger.info(f"$prompt: $cur%,d / $max%,d (${cur.toDouble / max * 100}%.1f%%) $left%s: $label%s")
        while(t0 + interval * step <= t) step += 1
      } else if(cur == max) {
        val spent = time((t - t0) / 1000L)
        logger.info(f"$prompt: $cur%,d / $max%,d (${cur.toDouble / max * 100}%.1f%%) finish in $spent%s")
      }
      true
    })
  }

  def fileProgress(file:File, charset:Charset, cacheDB:Database = null)(f:(String) => Unit):Unit = progress(file.getName, countLines(file)) { prog =>
    prog(0, "START")
    readBinary(file) { is =>
      Source.fromInputStream(is, charset.name()).getLines().zipWithIndex.foreach { case (line, i) =>
        f(line)
        prog(i + 1, line.take(50))
      }
    }
  }

  class NewLineCallbackReader(in:Reader, newLineListener:(Int) => Unit) extends FilterReader(in) {
    private[this] var line = 1

    override def read():Int = {
      val ch = super.read()
      if(ch == '\n') {
        line += 1
        newLineListener(line)
      }
      ch
    }

    override def read(cbuf:Array[Char]):Int = read(cbuf, 0, cbuf.length)

    override def read(cbuf:Array[Char], off:Int, len:Int):Int = {
      val length = super.read(cbuf, off, len)
      if(length >= 0) {
        for(i <- off until (off + length)) {
          if(cbuf(i) == '\n') {
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
