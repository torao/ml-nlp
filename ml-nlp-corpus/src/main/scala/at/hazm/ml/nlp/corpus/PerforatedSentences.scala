package at.hazm.ml.nlp.corpus

import at.hazm.core.db._
import at.hazm.ml.nlp.{Corpus, Morph, Paragraph}

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

  /**
    * 最短化された文を保持するための KVS。
    */
  private[this] val shortenedSentences = new db.KVS[Int, String](tableName("shortened_sentences"))

  /**
    * 最短文から削除する形態素の表現。
    */
  private[this] val StopWords = Set("、")

  /**
    * プレースホルダに置き換える品詞。
    */
  private[this] val POS2Sign = Map("名詞" -> "N", "動詞" -> "V", "形容詞" -> "A")

  def size:Int = shortenedSentences.size

  /**
    * 指定されたパラグラフを登録します。
    *
    * @param paragraph パラグラフ
    */
  def register(paragraph:Paragraph):Unit = {
    val morphs = corpus.vocabulary.getAll(paragraph.toIndices.distinct)
    paragraph.sentences.flatMap { sentence =>
      // 最短文の形態素シーケンスに分解
      sentence.breakdown().map(s => (sentence.id, s.clauses.flatMap(_.morphs.map(m => (m.morphId, morphs(m.morphId))))))
    }.map { case (sentenceId, sentence) =>
      (sentenceId, optimize(sentence))
    }.filter(_._2.nonEmpty).foreach { case (sentenceId, sentence) =>
      // データベースに登録
      val args = mutable.HashMap[String, Int]()
      val morphIds = sentence.map { case (morphId, morph) =>
        POS2Sign.get(morph.pos1) match {
          case Some(prefix) =>
            val key = s"$prefix${args.size}"
            args.put(key, morphId)
            key
          case None => morphId.toString
        }
      }.mkString(" ")

      // 最短文の登録
      val id = shortenedSentences.synchronized {
        val ids = shortenedSentences.getIds(morphIds)
        if(ids.isEmpty) {
          val id = shortenedSentences.size
          shortenedSentences.set(id, morphIds)
          id
        } else ids.head
      }

      // プレースホルダー部分に入る形態素の登録
      if(args.nonEmpty){
        placeholders.add(id, paragraph.id, sentenceId, args.toMap)
      }
    }
  }

  private[this] def optimize(sentence:Seq[(Int, Morph)]):Seq[(Int, Morph)] = {
    // 文中の不要な形態素を削除
    sentence.filter { s => !StopWords.contains(s._2.surface) }
  }


  /**
    *
    */
  object placeholders {

    private[this] val placeholderTable = tableName("placeholders")

    db.trx { con =>
      con.createTable(
        s"""$placeholderTable(
           |  id INTEGER NOT NULL PRIMARY KEY AUTO_INCREMENT,
           |  shorten_sentence_id INTEGER NOT NULL,
           |  paragraph_id INTEGER NOT NULL,
           |  sentence_id INTEGER NOT NULL,
           |  placeholder VARCHAR(6) NOT NULL,
           |  morph_id INTEGER NOT NULL)""".stripMargin)
      con.createIndex(s"${placeholderTable}_idx00 on $placeholderTable(shorten_sentence_id)")
      con.createIndex(s"${placeholderTable}_idx01 on $placeholderTable(paragraph_id)")
      con.createIndex(s"${placeholderTable}_idx02 on $placeholderTable(sentence_id)")
      con.createIndex(s"${placeholderTable}_idx03 on $placeholderTable(morph_id)")
    }

    def add(shortenSentenceId:Int, paragraphId:Int, sentenceId:Int, morphs:Map[String, Int]):Unit = db.trx { con =>
      val placeholder = morphs.map(_ => "(?,?,?,?,?)").mkString(",")
      val args = morphs.flatMap(m => Seq(shortenSentenceId, paragraphId, sentenceId, m._1, m._2)).toSeq
      con.exec(s"INSERT INTO $placeholderTable(shorten_sentence_id,paragraph_id,sentence_id,placeholder,morph_id) VALUES$placeholder", args:_*)
    }

  }

}
