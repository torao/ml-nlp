package at.hazm.core.db

import java.io.{File, FileInputStream, FileOutputStream}
import java.sql.Timestamp
import java.util.Date

import at.hazm.core.io.using

class BlobStore(val db:Database, val namespace:String) {

  db.trx { con =>
    con.exec(s"create table if not exists ${namespace}_files(path text not null primary key, last_modified datetime not null, content blob not null)")
  }

  case class Entry(path:String) {

    def exists:Boolean = db.trx { con =>
      con.head(s"select count(*) from ${namespace}_files where path=?", path)(_.getInt(1)) == 1
    }

    def save[T](f:(File) => T):T = {
      val file = File.createTempFile("blobstore", ".tmp")
      val result = f(file)

      val now = new Timestamp(new Date().getTime)
      val blob = using(new FileInputStream(file)) { in =>
        at.hazm.core.io.readAllBytes(in)
      }
      db.trx { con =>
        con.exec(s"insert or replace into ${namespace}_files(path, last_modified, content) values(?, ?, ?)", path, now, blob)
      }
      file.delete()

      result
    }

    def load[T](f:(File) => T):T = {
      val file = File.createTempFile("blobstore", ".tmp")
      file.deleteOnExit()
      try {
        db.trx { con =>
          val blob = con.head(s"select content from ${namespace}_files where path=?", path)(_.getBlob(1))
          using(blob.getBinaryStream, new FileOutputStream(file)) { (in, out) =>
            at.hazm.core.io.copy(in, out)
          }
          f(file)
        }
      } finally {
        file.delete()
      }
    }
  }

}
