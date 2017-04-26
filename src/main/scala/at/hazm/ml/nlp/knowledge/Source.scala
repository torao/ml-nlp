package at.hazm.ml.nlp.knowledge

import at.hazm.ml.io.Database
import at.hazm.ml.io.Database._

abstract class Source(val uri:String, val label:String)

object Source {
  val values = Seq(Wikipedia)

  private[this] val valuesMap = values.map(s => s.uri -> s).toMap
  def get(uri:String):Option[Source] = valuesMap.get(uri)
  def apply(uri:String):Source = valuesMap.apply(uri)

  class DB private[knowledge](db:Database) {
    db.trx { con =>
      con.createTable(
        """source(
          |  id integer not null primary key autoincrement,
          |  uri text not null unique,
          |  label text not null)""".stripMargin)
      values.foreach{ source =>
        registerSource(source.uri, source.label)
      }
    }

    def id(source: Source):Int = db.trx{ con =>
      con.head("select id from source where uri=?", source.uri)(_.getInt(1))
    }

    private[this] def registerSource(uri:String, label:String):Int = db.trx { con =>
      con.headOption("select id, label from source where uri=?", uri) { rs => (rs.getInt(1), rs.getString(2)) } match {
        case Some((id, ll)) =>
          if (label != ll) {
            con.exec("update source set label=? where id=?", label, id)
          }
          id
        case None =>
          con.exec("insert into source(uri, label) values(?, ?)", uri, label)
          con.head("select id from source where uri=?", uri)(_.getInt(1))
      }
    }

  }

}