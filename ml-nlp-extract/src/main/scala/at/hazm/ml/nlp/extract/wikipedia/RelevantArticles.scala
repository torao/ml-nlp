package at.hazm.ml.nlp.extract.wikipedia

import java.io.{File, FileOutputStream, OutputStreamWriter, PrintWriter}
import java.nio.charset.StandardCharsets
import java.sql.Connection

import at.hazm.core.db.{LegacyLocalDB, _}
import at.hazm.core.io.using

import scala.collection.mutable
import scala.io.Source

/**
  * データベースから特定のカテゴリに属する記事のみを抽出する。
  * {{{
  *   $ sbt "runMain at.hazm.ml.mlp.extract.wikipedia.RelevantArticles [dbname]"
  * }}}
  */
object RelevantArticles {
  def main(args:Array[String]):Unit = using(new LegacyLocalDB(new File(args.head))) { db =>
    val file = new File(s"relevant-categories.tsv")
    val categories = Source.fromFile(file, "UTF-8").getLines.map(_.trim()).filterNot(_.startsWith("#")).toSet
    db.trx { con =>
      val values = categories.flatMap { c =>
        con.headOption("SELECT id FROM categories WHERE title=?", c)(_.getInt(1))
      }.mkString(",")
      val pageIds = con.query(s"SELECT page_id FROM page_categories WHERE category_id IN($values)")(_.getInt(1)).toList.distinct
      System.out.println(f"${pageIds.length}%,d articles found")
      using(new PrintWriter(new OutputStreamWriter(new FileOutputStream("relevant-articles.tsv"), StandardCharsets.UTF_8))) { out =>
        pageIds.foreach { id =>
          con.headOption("SELECT title, content FROM articles WHERE id=?", id) { rs =>
            def n(text:String) = text.replaceAll("\\s+", " ").trim()

            out.println(s"$id\t${n(rs.getString(1))}\t${n(rs.getString(2))}")
            out.flush()
          }
        }
      }
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
