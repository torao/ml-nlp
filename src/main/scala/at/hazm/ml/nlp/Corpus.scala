package at.hazm.ml.nlp

import java.io.File
import java.sql.Connection

import at.hazm.ml.io.Database
import at.hazm.ml.io.Database._
import at.hazm.ml.nlp.pipeline.{ExhaustPipe, IntakePipe}

/**
  * 文書や単語などのテキストデータベース機能です。
  *
  * @param db テキストを保存するデータベース
  */
class Corpus(db:Database) {

  def this(file:File) = this(new Database(file))

  /**
    * ボキャブラリ (単語の集合) を表すクラスです。
    *
    * @param namespace 同一コーパス内で区別するための名前空間
    */
  class Vocabulary(namespace:String = "") {
    private[this] val table = if(namespace.isEmpty) "vocabulary" else s"vocabulary_$namespace"
    private[this] var _size = -1

    db.trx { con =>
      // コーパステーブルの作成
      con.createTable(s"$table(idx integer not null primary key, term text not null unique)")
      // コーパスの整合性を確認
      this._size = con.head(s"select count(*) from $table")(_.getInt(1))
      if(con.head(s"select count(*) from $table where idx<0 or idx>=?", _size)(_.getInt(1)) > 0) {
        throw new IllegalStateException(s"terms idx conflict in $table (term removed?)")
      }
    }

    /**
      * このボキャブラリに登録されている単語数を参照します。
      *
      * @return ボキャブラリの単語数
      */
    def size:Int = _size

    def get(index:Int):Option[String] = db.trx { con =>
      con.headOption(s"select term from $table where idx = ?", index)(_.getString(1))
    }

    def getAll(indices:Seq[Int]):Map[Int, String] = db.trx { con =>
      con.query(s"select idx, term from $table where idx in (${indices.mkString(",")})") { rs =>
        (rs.getInt(1), rs.getString(2))
      }.toMap
    }

    /**
      * 指定された単語をこのボキャブラリに登録します。
      *
      * @param term 登録する単語
      * @return 単語のインデックス
      */
    def register(term:String):Int = db.trx { con =>
      _register(con, term)
    }

    /**
      * 指定された単語をこのボキャブラリに登録します。
      *
      * @param terms 登録する単語
      * @return 単語に対するインデックスのマップ
      */
    def register(terms:Seq[String]):Map[String, Int] = db.trx { con =>
      val dterms = terms.distinct
      val in = dterms.map(_.replaceAll("\'", "\'\'")).mkString("'", "','", "'")
      val m1 = con.query(s"select idx, term from $table where term in ($in)") { rs =>
        (rs.getString(2), rs.getInt(1))
      }.toMap
      val m2 = dterms.collect { case term if !m1.contains(term) =>
        term -> register(term)
      }.toMap
      m1 ++ m2
    }

    private[this] def _register(con:Connection, term:String):Int = {
      con.headOption(s"select idx from $table where term=?", term)(_.getInt(1)) match {
        case Some(index) => index
        case None =>
          con.exec(s"insert into $table(idx, term) values(?, ?)", _size, term)
          _size += 1
          _size - 1
      }
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

    /**
      * このボキャブラリ上で単語を登録しインデックスに変換する入力パイプです。
      */
    def newIntake:IntakePipe[Seq[String], Seq[Int]] = IntakePipe({ terms:Seq[String] =>
      val indices = register(terms)
      terms.map(indices.apply)
    })

    /**
      * このボキャブラリでインデックスから単語への逆変換を行う出力パイプです。ボキャブラリに登録されていないインデックスを検出した場合は
      * 長さ 0 の文字列に置き換えられます。
      */
    def newExhaust:ExhaustPipe[Seq[Int], Seq[String]] = ExhaustPipe({ indices:Seq[Int] =>
      val terms = getAll(indices)
      indices.map(i => terms.getOrElse(i, ""))
    })
  }

}
