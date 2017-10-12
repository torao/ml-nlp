package at.hazm.ml.nlp.extract.wikipedia

import java.io._
import java.nio.charset.StandardCharsets

import at.hazm.core.io.using
import org.slf4j.LoggerFactory

import scala.io.Source

private[wikipedia] object ArticlesXML2CategoryTSV {
  private[this] val logger = LoggerFactory.getLogger(getClass.getName.dropRight(1))

  /**
    * `jawiki-20170801-pages-articles.xml.bz2` 形式の記事ファイルから記事ごとのカテゴリとリダイレクトを抽出して TSV 形式
    * で出力します。
    *
    * @param src 記事のダンプファイル
    * @param dst 記事ごとのカテゴリとリダイレクトを保存するファイル
    * @return 読み込んだ記事数
    */
  def extract(src:File, dst:File):Int = {
    var count = 0
    using(new PrintWriter(new OutputStreamWriter(new FileOutputStream(dst), StandardCharsets.UTF_8))) { out =>
      out.println("PAGE-ID\tTITLE\tREDIRECT\tCATEGORIES*")
      ArticlesXML.parse(src).foreach { article =>
        val redirect = Redirect.findFirstMatchIn(article.content).map(_.group(1)).getOrElse("").replaceAll("\\s+", " ").trim()
        val categories = extractCategories(article.content).distinct.sorted
        val cats = categories.map(_.replaceAll("\\s+", " ")).mkString("\t")
        val title = article.title.replaceAll("\\s+", " ").trim()
        out.println(s"${article.id}\t$title\t$redirect\t$cats")
        out.flush()
        count += 1
      }
    }
    count
  }

  /**
    * 指定されたカテゴリ TSV ファイルに含まれる記事数を参照します。
    *
    * @param dst `extract()` で出力したカテゴリ TSV ファイル
    * @return ファイルに保存されている記事数
    */
  def count(dst:File):Int = Source.fromFile(dst, "UTF-8").getLines().drop(1).size

  private[this] val Category = """(?i)\[\[(Category|カテゴリ):(.*?)\]\]""".r

  private[this] val Redirect = """(?i)\s*#REDIRECT\s*\[\[(.*?)\]\]\s*""".r

  private[this] def extractCategories(text:String):Seq[String] = Category.findAllMatchIn(text).map { m =>
    m.group(2).split("[|:]").head
  }.toSeq
}
