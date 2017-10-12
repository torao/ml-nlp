package at.hazm.ml.nlp

import java.sql.SQLException

import at.hazm.core.db.{Database, _}
import org.h2.api.ErrorCode

class TopicDB(db:Database, namespace:String = "") {
  private[this] val (topicsTable, topicDocuments) = {
    val prefix = if(namespace.isEmpty) "" else s"${namespace}_"
    (s"${prefix}topics", s"${prefix}topic_documents")
  }

  db.trx { con =>
    con.exec(
      s"""create table if not exists $topicsTable(
         |  id integer not null primary key,
         |  label varchar(100) not null
         |)""".stripMargin)
    con.exec(s"create index if not exists ${topicsTable}_idx01 on $topicsTable(label)")
    con.exec(
      s"""create table if not exists $topicDocuments(
         |  topic_id integer not null references $topicsTable(id) on delete cascade,
         |  document_id integer not null
         |)""".stripMargin)
    con.exec(s"create index if not exists ${topicDocuments}_idx01 on $topicDocuments(document_id)")
  }

  /**
    * このデータベースが保持している全てのトピックを削除します。
    */
  def clear():Unit = db.trx { con =>
    con.exec(s"delete from $topicsTable")
  }

  /**
    * 指定されたラベルの新しいトピックを定義します。
    *
    * @param label 登録するラベル
    * @return 新しいトピック
    */
  def newTopic(label:String):Topic = {
    def _newTopic(label:String):Int = try {
      db.trx { con =>
        val id = con.head(s"select count(*) from $topicsTable")(_.getInt(1))
        con.exec(s"insert into $topicsTable(id, label) values(?, ?)", id, label)
        id
      }
    } catch {
      case ex:SQLException if ex.getErrorCode == ErrorCode.DUPLICATE_KEY_1 =>
        _newTopic(label)
    }

    val id = _newTopic(label)
    Topic(id, label)
  }

  /**
    * このデータベースに定義されている全てのトピックを参照します。
    *
    * @return 登録されているトピック
    */
  def topics:Seq[Topic] = db.trx { con =>
    con.query(s"select id, label from $topicsTable"){ rs =>
      Topic(rs.getInt(1), rs.getString(2))
    }.toList
  }

  def addDocumentTopics(docId:Int, topicId:Int*):Unit = db.trx { con =>
    con.exec("insert into ")
  }

}
