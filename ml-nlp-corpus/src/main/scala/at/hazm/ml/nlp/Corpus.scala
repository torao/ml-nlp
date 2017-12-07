package at.hazm.ml.nlp

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStreamReader}
import java.nio.charset.StandardCharsets
import java.sql.ResultSet

import at.hazm.core.db._
import at.hazm.core.io.{readAllChars, using}
import at.hazm.ml.nlp.Corpus._RelativeDocumentType
import at.hazm.ml.nlp.corpus.{PerforatedSentences, Vocabulary}
import at.hazm.ml.nlp.model.{Morph, RelativeDocument}
import org.apache.commons.compress.compressors.bzip2.{BZip2CompressorInputStream, BZip2CompressorOutputStream}
import play.api.libs.json.Json

/**
  * 文書や形態素のデータベースです。
  *
  * @param db        テキストを保存するデータベース
  * @param namespace 同一データベース内でコーパスを区別するための名前空間
  */
class Corpus(val db:Database, val namespace:String) {

  // 名前空間が指定されていたらスキーマの作成
  if(namespace.nonEmpty) {
    db.trx(_.exec(s"CREATE SCHEMA IF NOT EXISTS $namespace"))
  }

  def this(db:Database) = this(db, "")

  /**
    * このコーパスで使用するテーブル名を作成します。
    *
    * @param name テーブルの識別子
    * @return 利用可能なテーブル名
    */
  private[this] def tableName(name:String):String = if(namespace.isEmpty) name else s"$namespace.$name"

  /**
    * ボキャブラリ (単語の集合) を表すクラスです。
    */
  val vocabulary:Vocabulary = new Vocabulary(db, tableName("vocabulary"))

  /**
    * このコーパスでインデックスづけられた文書を保存する KVS ストレージです。
    */
  val documents = new db.KVS[Int, RelativeDocument[Morph.Instance]](tableName("documents"))(_IntKeyType, _RelativeDocumentType)

  /**
    * このコーパスで最短文を保持するストレージです。
    */
  val perforatedSentences = new PerforatedSentences(db, tableName("perforated"), this)

}

object Corpus {
  private[this] type STORE_DOC = RelativeDocument[Morph.Instance]

  private[Corpus] object _RelativeDocumentType extends _ValueType[STORE_DOC] {
    val typeName:String = "BYTEA"

    def toStore(value:STORE_DOC):AnyRef = {
      val json = Json.stringify(value.toJSON)
      val baos = new ByteArrayOutputStream()
      using(new BZip2CompressorOutputStream(baos)) { out => out.write(json.getBytes(StandardCharsets.UTF_8)) }
      baos.toByteArray
    }

    def get(key:Any, rs:ResultSet, i:Int):STORE_DOC = {
      val bytes = rs.getBytes(i)
      val bais = new ByteArrayInputStream(bytes)
      val json = using(new InputStreamReader(new BZip2CompressorInputStream(bais), StandardCharsets.UTF_8)) { in =>
        readAllChars(in, bytes.length * 2)
      }
      RelativeDocument.fromJSON(Json.parse(json))
    }

    /**
      * 内容から ID を逆引きする操作は ID で同一性が決定するため ID をハッシュとする。
      */
    def hash(value:STORE_DOC):Int = value.id

    def equals(value1:STORE_DOC, value2:STORE_DOC):Boolean = value1.id == value2.id

    override def export(value:STORE_DOC):String = Json.stringify(value.toJSON)
  }

}