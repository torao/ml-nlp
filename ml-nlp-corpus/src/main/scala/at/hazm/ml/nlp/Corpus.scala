package at.hazm.ml.nlp

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, File, InputStreamReader}
import java.nio.charset.StandardCharsets
import java.sql.{PreparedStatement, ResultSet}
import java.util.concurrent.atomic.AtomicInteger

import at.hazm.core.db.{LocalDB, _}
import at.hazm.core.io.{readAllChars, using}
import at.hazm.ml.nlp.Corpus._ParagraphType
import org.apache.commons.compress.compressors.bzip2.{BZip2CompressorInputStream, BZip2CompressorOutputStream}
import play.api.libs.json.{JsObject, Json}

/**
  * 文書や形態素のデータベースです。
  *
  * @param db        テキストを保存するデータベース
  * @param namespace 同一データベース内でコーパスを区別するための名前空間
  */
class Corpus(val db:LocalDB, val namespace:String) {

  def this(db:LocalDB) = this(db, "")

  def this(file:File, namespace:String) = this(new LocalDB(file), namespace)

  def this(file:File) = this(new LocalDB(file), "")

  /**
    * このコーパスで使用するテーブル名を作成します。
    *
    * @param name テーブルの識別子
    * @return 利用可能なテーブル名
    */
  private[this] def tableName(name:String):String = if(namespace.isEmpty) name else s"${namespace}_$name"

  /**
    * ボキャブラリ (単語の集合) を表すクラスです。
    */
  object vocabulary {
    private[this] val table = tableName("morphs")

    /** ID 設定用のシーケンス */
    private[this] val sequence = new AtomicInteger()

    db.trx { con =>
      // コーパステーブルの作成
      con.createTable(
        s"""$table(id integer not null primary key, surface text not null,
           |pos1 text not null, pos2 text not null, pos3 text not null, pos4 text not null,
           |conj_type text not null, conj_form text not null, base text not null,
           |reading text not null, pronunciation text not null)""".stripMargin)
      con.exec(s"create unique index if not exists ${table}_idx00 on $table(surface, pos1, pos2, pos3, pos4)")
      this.sequence.set(con.head(s"select count(*) from $table")(_.getInt(1)))

      // コーパスの整合性を確認
      if(con.head(s"select count(*) from $table where id < 0 or id >= ?", sequence.get())(_.getInt(1)) > 0) {
        throw new IllegalStateException(s"vocabulary id conflict in $table (perhaps some morphs removed?)")
      }
    }

    /**
      * このボキャブラリに登録されている形態素数を参照します。
      *
      * @return ボキャブラリの形態素数
      */
    def size:Int = sequence.get()

    /**
      * 指定された ID を持つ形態素を参照します。
      *
      * @param id 形態素のID
      * @return 形態素
      */
    def get(id:Int):Option[Morph] = db.trx { con =>
      con.headOption(s"select * from $table where idx = ?", id)(rs2Morph)
    }

    /**
      * 指定された ID を持つ形態素を参照します。
      *
      * @param ids 形態素のID
      * @return ID -> 形態素 を示す Map
      */
    def getAll(ids:Seq[Int]):Map[Int, Morph] = db.trx { con =>
      con.query(s"select * from $table where idx in (${ids.distinct.mkString(",")})") { rs =>
        (rs.getInt("id"), rs2Morph(rs))
      }.toMap
    }

    /**
      * 指定された形態素がボキャブラリに登録されている場合、その ID (インデックス) を参照します。形態素が未登録の場合は負の値を返します。
      * 形態素は表現 (surface) と品詞 (pos) が一致している場合に同一とみなされます。
      *
      * @param morph ID を参照する形態素
      * @return 形態素の ID、未登録の場合は負の値
      */
    def indexOf(morph:Morph):Int = db.trx { con =>
      con.headOption(s"select id from $table where term=? and pos1=? and pos2=? and pos3=? and pos4=?",
        morph.surface, morph.pos1, morph.pos2, morph.pos3, morph.pos4)(_.getInt(1)).getOrElse(-1)
    }

    /**
      * 指定された形態素がボキャブラるに登録されている場合、その ID (インデックス) を参照します。返値は引数 `morphs` と同じ順序に並んだ
      * それぞれの ID です。未登録の形態素を検出した場合は負の値に置き換えられます。
      * 形態素は表現 (surface) と品詞 (pos) が一致している場合に同一とみなされます。
      *
      * @param morphs ID を参照する形態素
      * @return `morphs` と同じ順序に並んだインデックス
      */
    def indicesOf(morphs:Seq[Morph]):Seq[Int] = db.trx { con =>
      val existing = morphs.groupBy(_.key).values.map(_.head).grouped(20).map { ms =>
        val where = ms.map { morph =>
          s"surface=${literal(morph.surface)} and pos1=${literal(morph.pos1)} and pos2=${literal(morph.pos2)} and pos3=${literal(morph.pos3)} and pos4=${literal(morph.pos4)}"
        }.mkString("(", ") or (", ")")
        con.query(s"select * from $table where $where") { rs =>
          val id = rs.getInt("id")
          val morph = rs2Morph(rs)
          (morph.key, id)
        }.toMap
      }.reduceLeft(_ ++ _)
      morphs.map { morph =>
        existing.getOrElse(morph.key, -1)
      }
    }

    private[this] def rs2Morph(rs:ResultSet):Morph = Morph(
      surface = rs.getString("surface"),
      pos1 = rs.getString("pos1"),
      pos2 = rs.getString("pos2"),
      pos3 = rs.getString("pos3"),
      pos4 = rs.getString("pos4"),
      conjugationType = rs.getString("conj_type"),
      conjugationForm = rs.getString("conj_form"),
      baseForm = rs.getString("base"),
      reading = rs.getString("reading"),
      pronunciation = rs.getString("pronunciation")
    )

    private[this] def literal(text:String):String = "'" + text.replaceAll("'", "''") + "'"

    /**
      * 指定された形態素をこのボキャブラリに登録しその ID を返します。表現 (surface) と品詞 (pos) が等しい形態素が既に登録されている場合は
      *
      * @param morph 登録する形態素
      * @return 形態素の ID
      */
    def register(morph:Morph):Int = register(Seq(morph)).head

    /**
      * 指定された形態素をこのボキャブラリに登録しその ID を返します。返値は形態素 `morphs` の順序で並べられたそれぞれの ID です。
      * 表現 (surface) と品詞 (pos) が等しい形態素が既に登録されている場合は新規に登録せずその ID と置き換えられます。
      *
      * @param morphs 登録する形態素
      * @return 形態素の ID
      */
    def register(morphs:Seq[Morph]):Seq[Int] = {
      val unique = morphs.groupBy(_.key).mapValues(_.head)
      val uniqueTokens = unique.toSeq

      // 指定された形態素の既存の ID を参照 (未登録は負の値となるのでふるい分けする)
      val indices = indicesOf(uniqueTokens.map(_._2)).zip(uniqueTokens).map { case (id, (key, morph)) => (key, id, morph) }
      val existing = indices.filter(_._2 >= 0)

      // 新しく登録する形態素の SQL を構築
      val newbies = indices.filter(_._2 < 0).map(_._3).map { m =>
        val id = sequence.getAndIncrement()
        (m.key, id, s"($id,${literal(m.surface)},${literal(m.pos1)},${literal(m.pos2)},${literal(m.pos3)},${literal(m.pos4)},${literal(m.conjugationType)},${literal(m.conjugationForm)},${literal(m.baseForm)},${literal(m.reading)},${literal(m.pronunciation)})")
      }

      if(newbies.nonEmpty) {
        db.trx { con =>
          con.exec(s"insert into $table(id,surface,pos1,pos2,pos3,pos4,conj_type,conj_form,base,reading,pronunciation) values${newbies.map(_._3).mkString(",")}")
        }
      }

      // 既存の形態素 ID と新規登録形態素 ID の ID -> 形態素 Map を作成
      val map = (existing.map(x => (x._1, x._2)) ++ newbies.map(x => (x._1, x._2))).toMap
      morphs.map(t => map(t.key))
    }

    /**
      * 指定された単語に対するこのボキャブラリ上のインデックスを参照します。該当する単語が登録されていない場合は負の値を返します。
      *
      * @param term インデックスを検索する単語
      * @return 単語のインデックス、または負の値
      */
    def indexOf(term:String):Int = db.trx { con =>
      con.headOption(s"select idx from $table where term=?", term)(_.getInt(1)).getOrElse(-1)
    }

    /**
      * 指定された接頭辞から始まる単語を抽出します。
      *
      * @param prefix 検索する接頭辞
      * @return 取得した (ID,単語) のリスト
      */
    def prefixed(prefix:String):List[(Int, String)] = db.trx { con =>
      con.query(s"select idx, term from $table where term like '%' || ?", prefix) { rs => (rs.getInt(1), rs.getString(2)) }.toList
    }
  }

  /**
    * このコーパスで文書 (Paragraph) を保存する KVS ストレージです。
    */
  val paragraphs = new db.KVS[Int, Paragraph](tableName("paragraphs"))(_IntKeyType, _ParagraphType)

}

object Corpus {

  private[Corpus] object _ParagraphType extends _ValueType[Paragraph] {
    val typeName:String = "blob"

    def set(stmt:PreparedStatement, i:Int, value:Paragraph):Unit = {
      val json = Json.stringify(value.toJSON)
      val baos = new ByteArrayOutputStream()
      using(new BZip2CompressorOutputStream(baos)) { out => out.write(json.getBytes(StandardCharsets.UTF_8)) }
      stmt.setBytes(i, baos.toByteArray)
    }

    def get(rs:ResultSet, i:Int):Paragraph = {
      val bytes = rs.getBytes(i)
      val bais = new ByteArrayInputStream(bytes)
      val json = using(new InputStreamReader(new BZip2CompressorInputStream(bais), StandardCharsets.UTF_8)) { in =>
        readAllChars(in, bytes.length * 2)
      }
      Paragraph.fromJSON(Json.parse(json).as[JsObject])
    }

    def hash(value:Paragraph):Int = value.id

    def equals(value1:Paragraph, value2:Paragraph):Boolean = value1.id == value2.id
  }

}