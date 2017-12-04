package at.hazm.ml.nlp

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStreamReader}
import java.nio.charset.StandardCharsets
import java.sql.ResultSet
import java.util.concurrent.atomic.AtomicInteger

import at.hazm.core.db._
import at.hazm.core.io.{readAllChars, using}
import at.hazm.ml.nlp.Corpus._ParagraphType
import at.hazm.ml.nlp.corpus.PerforatedSentences
import org.apache.commons.compress.compressors.bzip2.{BZip2CompressorInputStream, BZip2CompressorOutputStream}
import play.api.libs.json.{JsObject, Json}

/**
  * 文書や形態素のデータベースです。
  *
  * @param db        テキストを保存するデータベース
  * @param namespace 同一データベース内でコーパスを区別するための名前空間
  */
class Corpus(val db:Database, val namespace:String) {

  def this(db:Database) = this(db, "")

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

    /** 形態素ごとの特徴ベクトル */
    val features = new db.KVS[Int, Seq[Double]](s"${table}_features")

    db.trx { con =>
      // コーパステーブルの作成
      con.createTable(
        s"""$table(id INTEGER NOT NULL PRIMARY KEY, hash INTEGER NOT NULL, surface VARCHAR(30) NOT NULL,
           |pos1 VARCHAR(15) NOT NULL, pos2 VARCHAR(15) NOT NULL, pos3 VARCHAR(15) NOT NULL, pos4 VARCHAR(15) NOT NULL,
           |conj_type VARCHAR(30) NOT NULL, conj_form VARCHAR(30) NOT NULL, base VARCHAR(30) NOT NULL,
           |reading VARCHAR(60) NOT NULL, pronunciation VARCHAR(60) NOT NULL)""".stripMargin)
      con.exec(s"CREATE UNIQUE INDEX IF NOT EXISTS ${table}_key ON $table(surface, pos1, pos2, pos3, pos4)")
      con.exec(s"CREATE INDEX IF NOT EXISTS ${table}_surface ON $table(surface)")
      con.exec(s"CREATE INDEX IF NOT EXISTS ${table}_pos1 ON $table(pos1)")
      con.exec(s"CREATE INDEX IF NOT EXISTS ${table}_pos2 ON $table(pos2)")
      con.exec(s"CREATE INDEX IF NOT EXISTS ${table}_pos3 ON $table(pos3)")
      con.exec(s"CREATE INDEX IF NOT EXISTS ${table}_pos4 ON $table(pos4)")
      con.exec(s"CREATE INDEX IF NOT EXISTS ${table}_hash ON $table(hash)")
      this.sequence.set(con.head(s"select count(*) from $table")(_.getInt(1)))

      // 構築時にコーパスの整合性を確認 (H2 Database は OR でつなげると異様に遅い)
      val invalidIds = con.query(s"SELECT id FROM $table WHERE id < 0")(_.getInt(1)).toList :::
        con.query(s"SELECT id FROM $table WHERE id >= ?", sequence.get())(_.getInt(1)).toList
      if(invalidIds.nonEmpty) {
        throw new IllegalStateException(s"vocabulary id conflict in $table (perhaps some morphs removed?): ${invalidIds.mkString(", ")}")
      }
    }

    /**
      * このボキャブラリに登録されている形態素数を参照します。返値の数値はこのインスタンスにキャッシュされている値です。インスタンスを
      * 経由せずボキャブラリテーブルを変更した場合はこの値から差異が発生します。
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
      con.headOption(s"SELECT * FROM $table WHERE id = ?", id)(rs2Morph)
    }

    /**
      * 指定された ID を持つ形態素を参照します。
      *
      * @param ids 形態素のID
      * @return ID -> 形態素 を示す Map
      */
    def getAll(ids:Seq[Int]):Map[Int, Morph] = db.trx { con =>
      val in = ids.map(_ => "?").mkString(",")
      con.query(s"SELECT * FROM $table WHERE id IN ($in)", ids:_*) { rs =>
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
      con.headOption(s"SELECT id FROM $table WHERE surface=? AND pos1=? AND pos2=? AND pos3=? AND pos4=?",
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
        val in1 = ms.map(_ => "?").mkString(",")
        val hashes = ms.map(m => Database.makeHash(m.key)).toSeq
        val in2 = ms.map(_ => "(?,?,?,?,?)").mkString(",")
        val params = ms.flatMap(morph => Seq(morph.surface, morph.pos1, morph.pos2, morph.pos3, morph.pos4)).toSeq
        con.query(s"SELECT * FROM $table WHERE hash IN ($in1) AND (surface,pos1,pos2,pos3,pos4) IN ($in2)", hashes ++ params:_*) { rs =>
          val id = rs.getInt("id")
          val morph = rs2Morph(rs)
          (morph.key, id)
        }.toMap
      }.reduceLeft(_ ++ _)
      morphs.map { morph =>
        existing.getOrElse(morph.key, -1)
      }
    }

    /**
      * 指定された表現 (surface) でボキャブラリに登録されている形態素を参照します。該当する形態素が未登録の場合は長さ 0 のシーケンスを
      * 返します。
      *
      * @param surface 形態素を参照する表現
      * @return 表現に一致する形態素とそのID
      */
    def instanceOf(surface:String):Seq[(Int, Morph)] = db.trx { con =>
      con.query(s"SELECT * FROM $table WHERE surface=?", surface) { rs =>
        (rs.getInt("id"), rs2Morph(rs))
      }.toList
    }

    /**
      * 指定された表現 (surface) でボキャブラリに登録されている形態素を参照します。該当する形態素が未登録の場合は長さ 0 のシーケンスを
      * 返します。
      *
      * @param surfaces 形態素を参照する表現
      * @return 表現に一致する形態素とそのID
      */
    def instanceOf(surfaces:Seq[String]):Seq[(Int, Morph)] = db.trx { con =>
      val in = surfaces.map(_ => "?").mkString(",")
      con.query(s"SELECT * FROM $table WHERE surface IN ($in)", surfaces) { rs =>
        (rs.getInt("id"), rs2Morph(rs))
      }.toList
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

    /**
      * 指定された形態素をこのボキャブラリに登録しその ID を返します。表現 (surface) と品詞 (pos) が等しい形態素が既に登録されている場合は
      *
      * @param morph 登録する形態素
      * @return 形態素の ID
      */
    def register(morph:Morph):Int = registerAll(Seq(morph)).head

    /**
      * 指定された形態素をこのボキャブラリに登録しその ID を返します。返値は形態素 `morphs` の順序で並べられたそれぞれの ID です。
      * 表現 (surface) と品詞 (pos) が等しい形態素が既に登録されている場合は新規に登録せずその ID と置き換えられます。
      *
      * @param morphs 登録する形態素
      * @return 形態素の ID
      */
    def registerAll(morphs:Seq[Morph]):Seq[Int] = synchronized {
      val unique = morphs.groupBy(_.key).mapValues(_.head)
      val uniqueTokens = unique.toSeq

      // 指定された形態素の既存の ID を参照 (未登録は負の値となるのでふるい分けする)
      val indices = indicesOf(uniqueTokens.map(_._2)).zip(uniqueTokens).map { case (id, (key, morph)) => (key, id, morph) }
      val existing = indices.filter(_._2 >= 0)

      // 新しく登録する形態素の SQL を構築
      val newbies = indices.filter(_._2 < 0).map(_._3).map { m =>
        val id = sequence.getAndIncrement()
        (m.key, id, m)
      }

      if(newbies.nonEmpty) {
        db.trx { con =>
          newbies.grouped(50).foreach { ms =>
            val values = ms.map(_ => "(?,?,?,?,?,?,?,?,?,?,?,?)").mkString(",")
            val params = ms.flatMap { case (_, id, m) =>
              val hash = Database.makeHash(m.key)
              Seq(id, hash, m.surface, m.pos1, m.pos2, m.pos3, m.pos4, m.conjugationType, m.conjugationForm, m.baseForm, m.reading, m.pronunciation)
            }
            con.exec(s"INSERT INTO $table(id,hash,surface,pos1,pos2,pos3,pos4,conj_type,conj_form,base,reading,pronunciation) VALUES$values", params:_*)
          }
        }
      }

      // 既存の形態素 ID と新規登録形態素 ID の ID -> 形態素 Map を作成
      val map = (existing.map(x => (x._1, x._2)) ++ newbies.map(x => (x._1, x._2))).toMap
      morphs.map(t => map(t.key))
    }

    /**
      * 指定された単語に対するこのボキャブラリ上のインデックスを参照します。該当する単語が登録されていない場合は負の値を返します。
      * 単語の表現に対して複数の ID が割り当てられている場合はどれか一つの ID を返します。
      *
      * @param term インデックスを検索する単語
      * @return 単語のインデックス、または負の値
      */
    def indexOf(term:String):Int = db.trx { con =>
      con.headOption(s"SELECT id FROM $table WHERE term=?", term)(_.getInt(1)).getOrElse(-1)
    }

    /**
      * 指定された単語に対するこのボキャブラリ上のインデックスを参照します。
      *
      * @param term インデックスを検索する単語
      * @return 単語のインデックス
      */
    def indicesOf(term:String):Seq[Int] = db.trx(_.query(s"SELECT id FROM $table WHERE term=?", term)(_.getInt(1)).toList)

    /**
      * 指定された接頭辞から始まる単語を抽出します。
      *
      * @param prefix 検索する接頭辞
      * @return 取得した (ID,単語) のリスト
      */
    def prefixed(prefix:String):List[(Int, String)] = db.trx { con =>
      con.query(s"SELECT id, term FROM $table WHERE term LIKE '%' || ?", prefix) { rs => (rs.getInt(1), rs.getString(2)) }.toList
    }
  }

  /**
    * このコーパスで文書 (Paragraph) を保存する KVS ストレージです。
    */
  val paragraphs = new db.KVS[Int, Paragraph](tableName("paragraphs"))(_IntKeyType, _ParagraphType)

  /**
    * このコーパスで最短文を保持するストレージです。
    */
  val perforatedSentences = new PerforatedSentences(db, tableName("perforated"), this)

}

object Corpus {

  private[Corpus] object _ParagraphType extends _ValueType[Paragraph] {
    val typeName:String = "blob"

    def toStore(value:Paragraph):AnyRef = {
      val json = Json.stringify(value.toJSON)
      val baos = new ByteArrayOutputStream()
      using(new BZip2CompressorOutputStream(baos)) { out => out.write(json.getBytes(StandardCharsets.UTF_8)) }
      baos.toByteArray
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

    override def export(value:Paragraph):String = Json.stringify(value.toJSON)
  }

}