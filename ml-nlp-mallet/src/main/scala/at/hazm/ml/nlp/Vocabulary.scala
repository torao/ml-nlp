package at.hazm.ml.nlp

import java.io.File

import at.hazm.core.db.Database
import at.hazm.core.db._

class Vocabulary(db:Database, namespace:String) {

  def this(db:Database) = this(db, "")

  def this(file:File, namespace:String) = this(new LocalDB(file), namespace)

  def this(file:File) = this(new LocalDB(file), "")

  private[this] val prefix = if(namespace.isEmpty) "" else s"${namespace}_"

  db.trx { con =>
    con.exec(s"CREATE TABLE IF NOT EXISTS ${prefix}article_morphs(id INTEGER NOT NULL PRIMARY KEY, content TEXT NOT NULL)")
    con.exec(s"CREATE TABLE IF NOT EXISTS ${prefix}corpus(id INTEGER NOT NULL PRIMARY KEY, morph TEXT NOT NULL UNIQUE)")

    // コーパスのインデックスが矛盾していないことを確認
    con.head(s"SELECT MIN(id), MAX(id), COUNT(*) FROM ${prefix}corpus"){ rs =>
      val min = rs.getLong(1)
      if(min != 0){
        throw new IllegalStateException(s"minimum vocabulary id must be zero: $min")
      }
      val max = rs.getLong(2)
      val count = rs.getLong(3)
      if(max != count){
        throw new IllegalStateException(s"maximum vocabulary id $max is not equals its count $count")
      }
    }
  }

  /**
    * このボキャブラリに登録されている形態素数を参照します。
    *
    * @return ボキャブラリの形態素数
    */
  //def size:Int = sequence.get()

  /**
    * 指定された ID を持つ形態素を参照します。
    *
    * @param id 形態素のID
    * @return 形態素
    */
  /*
  def get(id:Int):Option[Morph] = db.trx { con =>
    con.headOption(s"SELECT * FROM $table WHERE idx = ?", id)(rs2Morph)
  }
  */

}
