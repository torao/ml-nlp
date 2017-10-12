package at.hazm.ml.nlp.extract.wikipedia

import java.io.{File, FileOutputStream, OutputStreamWriter, PrintWriter}
import java.nio.charset.StandardCharsets
import java.sql.Connection

import at.hazm.core.db.{LegacyLocalDB, _}
import at.hazm.core.io.using

import scala.collection.mutable

/**
  * コマンドラインオプションで指定されたカテゴリを参照しているサブカテゴリを再帰的に出力します。出力ファイルは空白文字の
  * インデント数で再帰を表しています。
  * {{{
  *   $ sbt "runMain at.hazm.ml.mlp.extract.wikipedia.RelevantCategories [dbname] [max-depth] [category] ..."
  * }}}
  * このコマンドはカテゴリにもとづいて特定の話題を抽出するために、関係のないカテゴリを手作業で削除することを目的として
  * います。
  */
object RelevantCategories {
  def main(args:Array[String]):Unit = using(new LegacyLocalDB(new File(args.head))) { db =>
    val depth = args(1).toInt
    using(new PrintWriter(new OutputStreamWriter(new FileOutputStream(s"relevant-categories.tsv"), StandardCharsets.UTF_8))) { out =>
      out.println("# " + args.mkString(" "))
      val count = db.trx { con =>
        args.drop(2).map { cat => extract(out, con, cat, 0, depth) }.sum
      }
      System.out.println(f"$count%,d articles exported")
    }
  }

  private[this] def norm(text:String):String = text.toUpperCase.replaceAll("[\\s\u3000]+", " ").trim()

  private[this] def extract(out:PrintWriter, con:Connection, category:String, indent:Int, depth:Int, exists:mutable.HashSet[String] = mutable.HashSet(), stack:Set[String] = Set.empty):Int = {
    val cat = norm(category)
    if(!stack.contains(cat) && stack.size < depth && !exists.contains(cat)) {
      out.println(("  " * indent) + cat)
      exists.add(cat)
      // 指定されたカテゴリに所属するページ ID を抽出
      val ids = con.query("SELECT b.page_id FROM categories AS a, page_categories AS b WHERE a.title=? AND a.id=b.category_id", cat)(_.getInt(1)).toList
      // ページ ID とタイトルを抽出
      val titles = con.query(s"SELECT id, title FROM pages WHERE id IN(${ids.mkString(",")})") { rs =>
        (rs.getInt(1), rs.getString(2))
      }.toList
      // 指定されたカテゴリ直下のページを出力
      //      val count = titles.collect {
      //        case (id, title) if !title.startsWith("CATEGORY:") => id
      //      }.flatMap { id =>
      //        con.headOption("SELECT title, content FROM articles WHERE id=?", id) { rs =>
      //          (rs.getString(1), rs.getString(2))
      //        }.map { case (title, content) =>
      //          def n(text:String) = text.replaceAll("\\s+", " ").trim()
      //
      //          out.println(s"$id\t${n(title)}\t${n(content)}")
      //          1
      //        }
      //      }.sum
      //      System.out.println(f"$count%,d articles from $cat")
      // カテゴリページを再帰的に出力
      titles.collect {
        case (_, title) if title.startsWith("CATEGORY:") => title.substring(9).trim()
      }.map { ncat =>
        val count = extract(out, con, ncat, indent + 1, depth, exists, stack + cat)
        System.out.println(f"$count catetories from $ncat")
        count
      }.sum + 1
    } else 0
  }
}
