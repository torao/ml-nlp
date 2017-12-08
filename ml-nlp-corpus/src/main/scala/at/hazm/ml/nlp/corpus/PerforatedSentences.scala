package at.hazm.ml.nlp.corpus

import at.hazm.core.db._
import at.hazm.ml.nlp.Corpus
import at.hazm.ml.nlp.model.PerforatedSentence._
import at.hazm.ml.nlp.model.{Morph, POS, PerforatedDocument, PerforatedSentence, RelativeDocument}
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
  private[this] val shortenedSentences = new db.Index[Seq[Token]](perforatedTable, "perforated_id", "morphs")(_PerforatedTokensType)

  /**
    * 最短化された文が実際に存在する文章を保持するための KVS。
    */
  private[this] val shortenedInstances = new db.KVS[Int, PerforatedDocument](instanceTable, "document_id", "perforated_sentences")(_IntKeyType, _PerforatedDocumentType)

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
        val perforatedId = shortenedSentences.add(tokens)

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
    val sentences = perforate(corpus, doc).map { case (_, perforated) =>
      perforated.map { case (tokens, placeholders) =>
        transform(tokens, placeholders)
      }.filter(_ >= 0)
    }
    logger.debug(s" => ${sentences.map(_.mkString("[", ",", "]")).mkString("[", ",", "]")}")
    val pdoc = PerforatedDocument(doc.id, sentences.filter(_.nonEmpty))
    if(logger.isDebugEnabled) {
      logger.debug(s"$doc => ${pdoc.makeString(corpus)}")
    }
    pdoc
  }

  private[this] def transform(tokens:Seq[Token], placeholders:Map[Placeholder, Int]):Int = {
    val perforatedId = shortenedSentences.indexOf(tokens)
    lazy val debugSentence = PerforatedSentence(perforatedId, tokens).makeString(corpus.vocabulary, placeholders)
    if(perforatedId < 0) {
      logger.warn(s"未定義の文が含まれています: $debugSentence")
    } else {
      logger.debug(s"$debugSentence => $perforatedId")
    }
    perforatedId
  }

  private[this] def perforate(corpus:Corpus, doc:RelativeDocument[Morph.Instance]):Seq[(Int, Seq[(Seq[Token], Map[Placeholder, Int])])] = {
    val morphs = corpus.vocabulary.getAll(doc.tokens.map(_.morphId).distinct)

    doc.sentences.zipWithIndex.map { case (sentence, sentenceNum) =>
      // 最短文の形態素シーケンスに分解
      (sentenceNum, sentence
        .breakdown()
        .map(s => s.flatten.tokens.map(m => (m.morphId, morphs(m.morphId))))
        .map(s => PerforatedSentences.optimize(s))
        .filter(_._1.nonEmpty))
    }
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

  /** 最短文から削除する形態素の表現。 */
  private[this] val StopWords = Set("、", "。")

  /** 最短文から削除する形態素の品詞。 */
  private[this] val StopPOS1 = Set(POS.Filler, POS.Other).map(_.default.pos1)
  private[this] val StopPOS2 = Set((POS.Noun.default.pos1, "引用文字列"))

  /** プレースホルダに置き換える品詞。 */
  private[this] val POS2Sign = Seq(POS.Noun, POS.Verb, POS.Adjective).groupBy(_.default.pos1).mapValues(_.head)

  /** 日時を表すパターン */
  private[this] val DateTime = Seq(
    "\\d+年", "\\d+月", "\\d+日", "\\d+年\\d+月", "\\d+月\\d+日", "\\d+時", "\\d+分", "\\d+秒"
  ).map(_.r.pattern)

  /**
    * 指定された文をプレースホルダ化した最短文に変換します。不要な形態素は除去し、連続したプレースホルダは一つにまとめられます。
    *
    * @param sentence プレースホルダ化する文
    * @return プレースホルダ化したトークンとプレースホルダの形態素
    */
  private[PerforatedSentences] def optimize(sentence:Seq[(Int, Morph)]):(Seq[Token], Map[Placeholder, Int]) = {
    val tokens = mutable.Buffer[PerforatedSentence.Token]()
    val args = mutable.HashMap[Placeholder, Int]()
    var prePOS = ""
    sentence
      .filter(s => !StopWords.contains(s._2.surface)) // 除外する表現
      .filter(s => !StopPOS1.contains(s._2.pos1)) // 除外する品詞1
      .filter(s => !StopPOS2.contains((s._2.pos1, s._2.pos2))) // 除外する品詞2
      .map { case (id, token) =>
      (id, token match {
        case num@Morph(_, POS.Noun.label, "数", _, _) => num.copy(surface = "$NUMBER")
        case datetime@Morph(surface, POS.Noun.label, "固有名詞", "一般", _) if DateTime.exists(_.matcher(surface).matches()) =>
          datetime.copy(surface = "$DATETIME")
        case norm => norm
      })
    }
      .foreach { case (id, m) =>
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

  implicit object _PerforatedTokensType extends _ValueTypeForStringColumn[Seq[Token]] {
    override def from(text:String):Seq[Token] = text.split("\\s+").filter(_.nonEmpty).map {
      case morphId if Character.isDigit(morphId.head) => MorphId.fromString(morphId)
      case placeholder => Placeholder.fromString(placeholder)
    }.toSeq

    override def to(value:Seq[Token]):String = value.map(_.toString).mkString(" ")
  }

  implicit object _PerforatedDocumentType extends _ValueTypeForStringColumn[PerforatedDocument] {
    override def from(text:String):PerforatedDocument = Json.parse(text) match {
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

    override def to(value:PerforatedDocument):String = Json.stringify(JsArray(value.sentences.map(s => JsArray(s.map(i => JsNumber(i))))))
  }

}
