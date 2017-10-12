package at.hazm.ml.nlp.extract

import java.io._

import at.hazm.core.db._
import at.hazm.core.io.using
import at.hazm.core.util.Diagnostics.Progress
import at.hazm.ml.nlp.ja._
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
  * データベース内の記事本文を形態素解析しコーパスを作成します。
  * {{{
  *   $ sbt "run at.hazm.ml.nlp.extract.Tokenize F:\wikipedia\jawiki-YYYYMMDD-pages-articles.db.sqlite"
  * }}}
  */
object Tokenize {
  private[this] val logger = LoggerFactory.getLogger(getClass.getName.dropRight(1))

  def main(args:Array[String]):Unit = {
    val NamePattern = "([a-z]{2})wiki-(\\d{8}|latest)-pages-articles\\.db\\.sqlite".r
    val file = new File(args.head)
    val NamePattern(prefix, _) = file.getName
    if(!file.isFile) {
      help(s"specified file is not exist or not file: $file")
    }
    using(new LegacyLocalDB(file)) { db =>
      db.trx { con =>
        con.exec(s"CREATE TABLE IF NOT EXISTS article_morphs(id INTEGER NOT NULL PRIMARY KEY, content TEXT NOT NULL)")
        con.exec(s"CREATE TABLE IF NOT EXISTS corpus(id INTEGER NOT NULL PRIMARY KEY, morph TEXT NOT NULL UNIQUE)")

        // 形態素解析対象の記事 ID を参照
        val targets = (
          con.query("SELECT id FROM articles")(_.getInt(1)).toSet -- con.query("SELECT id FROM article_morphs")(_.getInt(1)).toSet
          ).toSeq

        // コーパスを取得
        val corpus = mutable.HashMap[String, Int](con.query("SELECT id, morph FROM corpus") { rs =>
          (rs.getString(2), rs.getInt(1))
        }.toList:_*)

        val prog = new Progress("tokenize", 0, targets.length)
        targets.grouped(1000).foreach { ids =>
          val newTokens = mutable.Buffer[(String, Int)]()
          val parsed = con.query(s"SELECT id, content FROM articles WHERE id IN (${ids.mkString(",")})") { rs =>
            (rs.getInt(1), rs.getString(2))
          }.toList.map { case (id, content) =>
            val tokens = combineNegatives(Kuromoji.tokenize(normalize(content))).filter { tk =>
              (tk.pos1 == "名詞" || tk.pos1 == "動詞" || tk.pos1 == "形容詞") && !tk.surface.matches("\\d+")
            }.map { tk =>
              s"${if(tk.baseForm.isEmpty) tk.surface else tk.baseForm}:${tk.pos1}"
            }

            // 形態素を ID にマップし新しい形態素を記録
            val ids = tokens.map { tk =>
              corpus.getOrElseUpdate(tk, {
                val id = corpus.size
                newTokens.append((tk, id))
                id
              })
            }
            prog.report(tokens.take(20).mkString(" "))
            (id, ids)
          }
          if(newTokens.nonEmpty){
            con.exec(s"INSERT INTO corpus(id, morph) VALUES${
              newTokens.map { case (tk, id) => s"($id, '${sql(tk)}')" }.mkString(",")
            }")
          }
          con.exec(s"INSERT INTO article_morphs(id, content) VALUES${
            parsed.map{ case (id, tokenIds) => s"($id,'${tokenIds.mkString(" ")}')"}.mkString(",")
          }")
        }
      }

    }

  }

  /**
    * 指定された文字列を SQL に埋め込み可能な形式にエスケープします。
    *
    * @param text エスケープする文字列
    * @return
    */
  private[this] def sql(text:String):String = text.replace("\\", "\\\\").replace("'", "''")

  /**
    * コマンドラインオプションを表示して終了します。
    *
    * @param message エラーメッセージ
    * @return リターンコード1で終了
    */
  private[this] def help(message:String = ""):Nothing = {
    if(message.nonEmpty) {
      System.err.println(s"ERROR: $message")
    }
    System.err.println(
      s"""USAGE: sbt 'runMain ${
        getClass.getName.dropRight(1)
      } jawiki-YYYYMMDD-pages-articles.xml.bz2 topics.h2'
         |
       """.stripMargin)
    System.exit(1)
    throw new IllegalStateException()
  }
}
