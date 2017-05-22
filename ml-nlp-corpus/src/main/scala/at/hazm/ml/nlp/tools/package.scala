package at.hazm.ml.nlp

import java.io.File
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.StandardOpenOption
import java.sql.Timestamp

import at.hazm.core.db._
import at.hazm.core.io.{readBinary, using}

import scala.annotation.tailrec

/**
  * Created by Takami Torao on 2017/05/22.
  */
package object tools {

  def countLines(file:File, cacheDB:LocalDB = null):Int = Option(cacheDB).flatMap { db =>
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
      System.err.println(": DONE")
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

}
