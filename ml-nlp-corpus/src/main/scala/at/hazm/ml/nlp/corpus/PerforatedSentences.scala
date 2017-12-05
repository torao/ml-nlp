package at.hazm.ml.nlp.corpus

import at.hazm.core.db._
import at.hazm.ml.nlp.Corpus
import at.hazm.ml.nlp.model.{Morph, RelativeDocument}

import scala.collection.mutable

/**
  * 形態素解析された文を係り受け関係から最短化し、名詞、動詞、形容詞を分離した文を保持します。
  *
  * @param db        データベース
  * @param namespace 名前空間
  * @param corpus    コーパス
  */
class PerforatedSentences private[nlp](db:Database, namespace:String, corpus:Corpus) {

  /**
    * このコーパスで使用するテーブル名を作成します。
    *
    * @param name テーブルの識別子
    * @return 利用可能なテーブル名
    */
  private[this] def tableName(name:String):String = if(namespace.isEmpty) name else s"${namespace}_$name"

  private[this] val perforatedTable = tableName("sentences")

  /**
    * 最短化された文を保持するための KVS。
    */
  private[this] val shortenedSentences = new db.Index[String](perforatedTable, unique = true)

  /**
    * 最短文から削除する形態素の表現。
    */
  private[this] val StopWords = Set("、", "。")

  /**
    * プレースホルダに置き換える品詞。
    */
  private[this] val POS2Sign = Map("名詞" -> "N", "動詞" -> "V", "形容詞" -> "A")

  def size:Int = shortenedSentences.size

  /**
    * 指定されたパラグラフを登録します。
    *
    * @param doc パラグラフ
    */
  def register(doc:RelativeDocument[Morph.Instance]):Unit = {
    // プレースホルダに置き換える形態素を決定するために形態素を取得する
    val morphs = corpus.vocabulary.getAll(doc.tokens.map(_.morphId).distinct)
    doc.sentences.zipWithIndex.flatMap { case (sentence, sentenceId) =>
      // 最短文の形態素シーケンスに分解
      sentence.breakdown().map { s =>
        s.flatten.tokens.map(m => (m.morphId, morphs(m.morphId)))
      }.map(optimize).filter(_._1.nonEmpty).zipWithIndex.map(t => (doc.id, sentenceId, t._2, t._1._1, t._1._2))
    }.foreach { case (docId, sentId, pfId, keys, args) =>

      // 最短文の登録
      val id = shortenedSentences.register(keys)

      // プレースホルダー部分に入る形態素の登録
      if(args.nonEmpty) {
        chads.add(id, docId, sentId, args)
      }
    }
  }

  private[this] def optimize(sentence:Seq[(Int, Morph)]):(String, Map[String, Int]) = {
    val buf = mutable.Buffer[String]()
    val args = mutable.HashMap[String, Int]()
    var prePOS = ""
    sentence.filter(s => !StopWords.contains(s._2.surface)).foreach { case (id, m) =>
      // 連続したプレースホルダ対象の品詞を削除
      if(!POS2Sign.contains(m.pos1) || prePOS != m.pos1) {
        val key = POS2Sign.get(m.pos1) match {
          case Some(prefix) =>
            val key = s"$prefix${args.size}"
            args.put(key, id)
            key
          case None => id.toString
        }
        buf.append(key)
        prePOS = m.pos1
      }
    }
    (buf.mkString(" "), args.toMap)
  }


  /**
    *
    */
  object chads {

    private[this] val placeholderTable = tableName("chads")

    db.trx { con =>
      con.createTable(
        s"""$placeholderTable(
           |  id SERIAL NOT NULL PRIMARY KEY,
           |  document_id INTEGER NOT NULL,
           |  sentence_id INTEGER NOT NULL,
           |  perforated_id INTEGER NOT NULL,
           |  placeholder VARCHAR(6) NOT NULL,
           |  morph_id INTEGER NOT NULL)""".stripMargin)
      con.createIndex(s"${placeholderTable}_idx00 on $placeholderTable(document_id)")
      con.createIndex(s"${placeholderTable}_idx01 on $placeholderTable(sentence_id)")
      con.createIndex(s"${placeholderTable}_idx02 on $placeholderTable(perforated_id)")
      con.createIndex(s"${placeholderTable}_idx03 on $placeholderTable(morph_id)")
    }

    def add(shortenSentenceId:Int, paragraphId:Int, sentenceId:Int, morphs:Map[String, Int]):Unit = db.trx { con =>
      val placeholder = morphs.map(_ => "(?,?,?,?,?)").mkString(",")
      val args = morphs.flatMap(m => Seq(shortenSentenceId, paragraphId, sentenceId, m._1, m._2)).toSeq
      con.exec(s"INSERT INTO $placeholderTable(perforated_id,document_id,sentence_id,placeholder,morph_id) VALUES$placeholder", args:_*)
    }

    def maxDocId:Int = db.trx { con =>
      con.headOption(s"SELECT MAX(document_id) FROM $placeholderTable")(_.getInt(1)).getOrElse(-1)
    }

    def docSize:Int = db.trx { con =>
      con.headOption(s"SELECT COUNT(document_id) FROM $placeholderTable GROUP BY document_id")(_.getInt(1)).getOrElse(0)
    }

  }

}
