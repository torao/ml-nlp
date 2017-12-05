package at.hazm.ml.nlp

import java.io.File
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.StandardOpenOption
import java.sql.Timestamp

import at.hazm.core.db._
import at.hazm.core.io.{readBinary, using}

import scala.annotation.tailrec

package object tools {

  def countLines(file:File):Int = {
    val cacheFile = new File(".ml-nlp.cache")
    val db = new LocalDB(cacheFile)
    // 行数が保存されていてファイルに変更がなければそれを返す
    db.trx { con =>
      con.createTable(s"file_info(name VARCHAR(${8 * 1024}) NOT NULL PRIMARY KEY, length BIGINT NOT NULL, last_modified TIMESTAMP NOT NULL, lines INTEGER NOT NULL)")
      con.headOption("SELECT lines FROM file_info WHERE name=? AND length=? AND last_modified=?",
        file.getCanonicalPath, file.length(), new Timestamp(file.lastModified()))(_.getInt(1))
    }.getOrElse {
      val lines = if(file.getName.endsWith(".gz")) {
        System.err.print(s"行数をカウントしています [${file.getName}] ")
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
        System.err.println(": 完了")
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
      db.trx { con =>
        con.exec("DELETE FROM file_info WHERE name=?", file.getCanonicalPath)
        con.exec("INSERT INTO file_info(name, length, last_modified, lines) VALUES(?, ?, ?, ?)",
          file.getCanonicalPath, file.length(), new Timestamp(file.lastModified()), lines
        )
      }

      lines
    }
  }

}
