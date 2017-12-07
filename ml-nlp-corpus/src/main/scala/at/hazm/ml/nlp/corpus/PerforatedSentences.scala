package at.hazm.ml.nlp.corpus

import java.sql.ResultSet

import at.hazm.core.db._
import at.hazm.ml.nlp.Corpus
import at.hazm.ml.nlp.model.PerforatedSentence._
import at.hazm.ml.nlp.model.{Morph, PerforatedDocument, PerforatedSentence, RelativeDocument}
import org.slf4j.LoggerFactory
import play.api.libs.json.{JsArray, JsNumber, Json}

import scala.collection.mutable

/**
  * 形態素解析された文を係り受け関係から最短化し、名詞、動詞、形容詞を分離した文を保持します。
  *
  * @param db        データベース
  * @param namespace 名前空間
  * @param corpus    コーパス
  */
class PerforatedSentences private[nlp](db:Database, namespace:String, corpus:Corpus) {

  import PerforatedSentences._

  /**
    * このコーパスで使用するテーブル名を作成します。
    *
    * @param name テーブルの識別子
    * @return 利用可能なテーブル名
    */
  private[this] def tableName(name:String):String = if(namespace.isEmpty) name else s"${namespace}_$name"

  private[this] val perforatedTable = tableName("sentences")
  private[this] val instanceTable = tableName("instance")

  /**
    * 最短化された文の定義を保持するための KVS。
    */
  private[this] val shortenedSentences = new db.Index[Seq[Token]](perforatedTable, "perforated_id", "morphs", unique = true)(_PerforatedTokensType)

  /**
    * 最短化された文が実際に存在する文章を保持するための KVS。
    */
  private[this] val shortenedInstances = new db.KVS[Int, PerforatedDocument](instanceTable, "document_id", "perforated_sentences")(_IntKeyType, _PerforatedDocumentType)

  /**
    * 最短文から削除する形態素の表現。
    */
  private[this] val StopWords = Set("、", "。")

  /**
    * プレースホルダに置き換える品詞。
    */
  private[this] val POS2Sign = Map("名詞" -> Noun, "動詞" -> Verb, "形容詞" -> Adjective)

  def size:Int = shortenedSentences.size

  def documentSize:Int = shortenedInstances.size

  def toDocumentCursor:Cursor[(Int, PerforatedDocument)] = shortenedInstances.toCursor

  def apply(id:Int):PerforatedSentence = {
    val tokens = shortenedSentences(id)
    PerforatedSentence(id, tokens)
  }

  /**
    * 係り受け解析済みの文書を最短文に変換して登録します。
    *
    * @param doc 係り受け解析済みの文書
    */
  def register(doc:RelativeDocument[Morph.Instance]):PerforatedDocument = {
    val sentences = perforate(corpus, doc).map { case (sentenceNum, perforated) =>
      perforated.map { case (tokens, args) =>

        // 最短文の登録
        val perforatedId = shortenedSentences.register(tokens)

        // プレースホルダー部分に入る形態素の登録
        if(args.nonEmpty) {
          chads.add(perforatedId, doc.id, sentenceNum, args)
        }
        perforatedId
      }
    }

    // 文書ごとの最短文のシーケンスを登録
    val pdoc = PerforatedDocument(doc.id, sentences)
    shortenedInstances.set(doc.id, pdoc)
    pdoc
  }

  def transform(doc:RelativeDocument[Morph.Instance]):PerforatedDocument = {
    val error = mutable.Buffer[String]()
    val sentences = perforate(corpus, doc).map { case (_, perforated) =>
      perforated.map { case (tokens, _) =>
        val perforatedId = shortenedSentences.indexOf(tokens)
        if(perforatedId < 0) {
          error.append("undefined sentence: " + tokens.map {
            case MorphId(morphId) =>
              corpus.vocabulary(morphId).surface
            case p:Placeholder => s"[${p.pos.symbol}]"
          }.mkString("."))
        }
        perforatedId
      }.filter(_ >= 0)
    }

    // 発生したエラーをまとめてログ出力
    if(error.nonEmpty) {
      error.foreach(msg => logger.warn(msg))
    }

    PerforatedDocument(doc.id, sentences)
  }

  private[this] def perforate(corpus:Corpus, doc:RelativeDocument[Morph.Instance]):Seq[(Int, Seq[(Seq[Token], Map[Placeholder, Int])])] = {
    val morphs = corpus.vocabulary.getAll(doc.tokens.map(_.morphId).distinct)

    doc.sentences.zipWithIndex.map { case (sentence, sentenceNum) =>
      // 最短文の形態素シーケンスに分解
      (sentenceNum, sentence
        .breakdown()
        .map(s => s.flatten.tokens.map(m => (m.morphId, morphs(m.morphId))))
        .map(s => optimize(s))
        .filter(_._1.nonEmpty))
    }
  }

  private[this] def optimize(sentence:Seq[(Int, Morph)]):(Seq[Token], Map[Placeholder, Int]) = {
    val tokens = mutable.Buffer[PerforatedSentence.Token]()
    val args = mutable.HashMap[Placeholder, Int]()
    var prePOS = ""
    sentence.filter(s => !StopWords.contains(s._2.surface)).foreach { case (id, m) =>
      // 連続したプレースホルダ対象の品詞を削除
      if(!POS2Sign.contains(m.pos1) || prePOS != m.pos1) {
        val token = POS2Sign.get(m.pos1) match {
          case Some(pos) =>
            val num = args.size
            val placeholder = Placeholder(num, pos)
            args.put(placeholder, id)
            placeholder
          case None => MorphId(id)
        }
        tokens.append(token)
        prePOS = m.pos1
      }
    }
    (tokens, args.toMap)
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
      con.createIndex(s"$placeholderTable(document_id)")
      con.createIndex(s"$placeholderTable(sentence_id)")
      con.createIndex(s"$placeholderTable(perforated_id)")
      con.createIndex(s"$placeholderTable(morph_id)")
      con.createIndex(s"$placeholderTable(document_id, sentence_id, perforated_id, placeholder)", unique = true)
    }

    def add(shortenSentenceId:Int, paragraphId:Int, sentenceId:Int, morphs:Map[Placeholder, Int]):Unit = db.trx { con =>
      val placeholder = morphs.map(_ => "(?,?,?,?,?)").mkString(",")
      val args = morphs.flatMap(m => Seq(shortenSentenceId, paragraphId, sentenceId, m._1.toString, m._2)).toSeq
      con.exec(
        s"""INSERT INTO $placeholderTable(perforated_id, document_id, sentence_id, placeholder, morph_id)
           | VALUES$placeholder
           | ON CONFLICT (document_id, sentence_id, perforated_id, placeholder) DO UPDATE SET
           |  perforated_id=EXCLUDED.perforated_id,
           |  document_id=EXCLUDED.document_id,
           |  sentence_id=EXCLUDED.sentence_id,
           |  placeholder=EXCLUDED.placeholder,
           |  morph_id=EXCLUDED.morph_id""".stripMargin, args:_*)
    }

    def maxDocId:Int = db.trx { con =>
      con.headOption(s"SELECT MAX(document_id) FROM $placeholderTable")(_.getInt(1)).getOrElse(-1)
    }

  }

}

object PerforatedSentences {
  private[PerforatedSentences] val logger = LoggerFactory.getLogger(classOf[PerforatedSentence])

  implicit object _PerforatedTokensType extends _ValueType[Seq[Token]] {
    override val typeName:String = "TEXT"

    override def toStore(value:Seq[Token]):String = value.map(_.toString).mkString(" ")

    override def get(key:Any, rs:ResultSet, i:Int):Seq[Token] = {
      val value = rs.getString(i)
      value.split("\\s+").filter(_.nonEmpty).map {
        case morphId if Character.isDigit(morphId.head) => MorphId.fromString(morphId)
        case placeholder => Placeholder.fromString(placeholder)
      }.toSeq
    }

    override def hash(value:Seq[Token]):Int = value.map(_.hashCode()).sum

    override def equals(value1:Seq[Token], value2:Seq[Token]):Boolean = value1.zip(value2).forall { case (a, b) => a == b }
  }

  implicit object _PerforatedDocumentType extends _ValueType[PerforatedDocument] {
    override val typeName:String = "TEXT"

    override def toStore(value:PerforatedDocument):String = {
      Json.stringify(JsArray(value.sentences.map(s => JsArray(s.map(i => JsNumber(i))))))
    }

    override def get(key:Any, rs:ResultSet, i:Int):PerforatedDocument = {
      val value = rs.getString(i)
      Json.parse(value) match {
        case JsArray(arr) =>
          PerforatedDocument(-1, arr.map {
            case JsArray(pss) =>
              pss.map {
                case JsNumber(num) => num.toInt
                case unexpected =>
                  throw new IllegalArgumentException(s"unexpected perforated sentence id: ${Json.stringify(unexpected)}")
              }
            case unexpected =>
              throw new IllegalArgumentException(s"unexpected perforated sentence ids: ${Json.stringify(unexpected)}")
          })
        case unexpected =>
          throw new IllegalArgumentException(s"unexpected perforated document: ${Json.stringify(unexpected)}")
      }
    }

    override def hash(value:PerforatedDocument):Int = value.id

    override def equals(value1:PerforatedDocument, value2:PerforatedDocument):Boolean = value1.id == value2.id
  }

}