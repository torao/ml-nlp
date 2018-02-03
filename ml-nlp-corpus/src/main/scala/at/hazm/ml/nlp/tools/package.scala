package at.hazm.ml.nlp

import java.io.File
import java.lang.management.ManagementFactory
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.StandardOpenOption
import java.sql.Timestamp

import at.hazm.core.db._
import at.hazm.core.io.{readBinary, using}

import scala.annotation.tailrec

package object tools {

  /**
    * 並列処理に利用可能な CPU 数。
    */
  val CPUs:Int = ManagementFactory.getOperatingSystemMXBean.getAvailableProcessors

  /**
    * 処理の進捗を永続的なストレージに保持するクラス。アプリケーションの再実行時に前回の続きから処理を開始したり、前回の処理を無視して
    * 新規に再実行することを目的とする。
    */
  object Resume extends AutoCloseable {
    // TODO 設計考慮中
    val cacheFile = new File(".ml-nlp.resume")
    lazy val db = new PortableDB(cacheFile)

    def close():Unit = {
      db.close()
    }
  }

  object cache {
    val cacheFile = new File(".ml-nlp.cache")

    using(new PortableDB(cacheFile)) { db =>
      // 行数が保存されていてファイルに変更がなければそれを返す
      db.trx { con =>
        con.createTable(s"file_info(name VARCHAR(${8 * 1024}) NOT NULL PRIMARY KEY, length BIGINT NOT NULL, last_modified TIMESTAMP NOT NULL, lines INTEGER NOT NULL)")
      }
    }

    def execIfModified(file:File)(f:(File) => Long):Long = using(new PortableDB(cacheFile)) { db =>
      val fileName = file.getCanonicalPath
      db.trx { con =>
        con.headOption("SELECT lines FROM file_info WHERE name=? AND length=? AND last_modified=?",
          fileName, file.length(), new Timestamp(file.lastModified()))(_.getLong(1))
      }.getOrElse {
        val value = f(file)
        // 行数を保存
        db.trx { con =>
          con.exec("DELETE FROM file_info WHERE name=?", fileName)
          con.exec("INSERT INTO file_info(name, length, last_modified, lines) VALUES(?, ?, ?, ?)",
            fileName, file.length(), new Timestamp(file.lastModified()), value
          )
        }
        value
      }
    }
  }

  @deprecated
  def countLines(file:File):Long = cache.execIfModified(file) { _:File =>
    if(file.getName.endsWith(".gz")) {
      System.err.print(s"行数をカウントしています [${file.getName}] ")
      System.err.flush()
      var period = 0
      val max = file.length()
      val count = readBinary(file, 512 * 1024, { (cur) =>
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
  }

}
