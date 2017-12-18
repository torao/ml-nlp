package at.hazm.ml.nlp

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStreamReader}
import java.nio.charset.StandardCharsets

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

  /**
    * ドキュメントの保存と参照を行うための KVS を作成します。
    *
    * @param name ドキュメントストアの名前
    * @param _vt  値の変換処理
    * @tparam T 値の型
    * @return KVS
    */
  def newDocumentStore[T](name:String)(implicit _vt:_ValueType[T]):Database#KVS[Int, T] = {
    new db.KVS[Int, T](tableName(s"doc_$name"))(_IntKeyType, _vt)
  }

}

object Corpus {
  private[this] type STORE_DOC = RelativeDocument[Morph.Instance]

  private[Corpus] object _RelativeDocumentType extends _ValueTypeForBinaryColumn[STORE_DOC] {
    override def from(bytes:Array[Byte]):STORE_DOC = {
      val bais = new ByteArrayInputStream(bytes)
      val json = using(new InputStreamReader(new BZip2CompressorInputStream(bais), StandardCharsets.UTF_8)) { in =>
        readAllChars(in, bytes.length * 2)
      }
      RelativeDocument.fromJSON(Json.parse(json))
    }

    override def to(value:STORE_DOC):Array[Byte] = {
      val json = Json.stringify(value.toJSON)
      val baos = new ByteArrayOutputStream()
      using(new BZip2CompressorOutputStream(baos)) { out => out.write(json.getBytes(StandardCharsets.UTF_8)) }
      baos.toByteArray
    }

    override def export(value:STORE_DOC):String = Json.stringify(value.toJSON)
  }

}