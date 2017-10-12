package at.hazm.ml.nlp.extract.wikipedia

import java.io._
import java.sql.Connection

import at.hazm.core.db._
import at.hazm.core.io.using
import at.hazm.core.util.Diagnostics.Progress
import org.slf4j.LoggerFactory

import scala.io.Source

/**
  * Wikipedia の記事データ `jawiki-YYYYMMDD-pages-articles.xml.bz2` を読み込んでページ ID ごとにタイトル、リダイレクト、
  * カテゴリを抽出し TSV ファイルに出力します。またその TSV ファイルから SQLite3 形式のデータベースを作成し、同じ
  * ディレクトリに存在する `wikipedia/` ディレクトリを wikiextract されたデータとしてデータベースに取り込みます。
  * {{{
  *   $ sbt "runMain F:\wikipedia\jawiki-YYYYMMDD-pages-articles.xml.bz2"
  * }}}
  * 巨大な XML を解析するため実行オプション
  * `-DtotalEntitySizeLimit=2147480000 -Djdk.xml.totalEntitySizeLimit=2147480000` が必要です。
  */
object Extract {
  private[this] val logger = LoggerFactory.getLogger(getClass.getName.dropRight(1))

  def main(args:Array[String]):Unit = {
    val NamePattern = "([a-z]{2}wiki-(\\d{8}|latest))-pages-articles\\.xml\\.bz2".r
    val (prefix, _) = args.headOption.map(f => new File(f).getName).collect {
      case NamePattern(p, i) => (p, i)
    }.getOrElse {
      help("Please specify wikipedia dump file compressed by bzip2, such as jawiki-20170801-pages-articles.xml.bz2")
    }
    val src = new File(args.head)
    if(!src.isFile) {
      help(s"specified file is not exist or not file: $src")
    }
    val dir = src.getParentFile
    val dst = new File(dir, s"$prefix-pages-categories.txt")

    // カテゴリとリダイレクトの抽出処理
    val size = if(!dst.isFile) {
      ArticlesXML2CategoryTSV.extract(src, dst)
    } else {
      logger.info(s"skip to extract categories from xml.gz: file exists: $dst")
      ArticlesXML2CategoryTSV.count(dst)
    }

    val dbFile = new File(dir, s"$prefix-pages-articles.db.sqlite")
    using(new LegacyLocalDB(dbFile)) { db =>

      // カテゴリとリダイレクトを正規化してデータベースへ保存
      db.trx { con =>
        importCategories(con, dst, size)
      }

      // 全ての記事内容を正規化してデータベースへ保存
      db.trx { con =>
        con.exec(s"CREATE TABLE IF NOT EXISTS articles(id INTEGER NOT NULL PRIMARY KEY, title TEXT NOT NULL, content TEXT NOT NULL)")
        val root = new File(dir, s"$prefix-pages-articles")
        if(root.isDirectory) {
          val files = root.listFiles().filter(_.isDirectory).flatMap(_.listFiles()).filter(_.isFile)
          val prog = new Progress(root.getName, 0, files.length)
          files.zipWithIndex.foreach { case (file, i) =>
            importDocument(con, file)
            prog.report(i + 1, file.getName)
          }
        } else {
          logger.warn(s"wikiextractor directory is not found: $root")
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
    * 指定された文字列をタイトル検索可能な形式に正規化します。
    *
    * @param text 変換する文字列
    * @return 正規化した文字列
    */
  private[this] def norm(text:String):String = text.toUpperCase.replaceAll("[\\s\u3000]+", " ").trim()

  /**
    * 指定されたファイルからデータベースへページとリダイレクト、カテゴリを取り込みます。
    *
    * @param con  データベース接続
    * @param dst  取り込み元のファイル
    * @param size データ件数
    */
  private[this] def importCategories(con:Connection, dst:File, size:Int):Unit = {
    con.exec(s"CREATE TABLE IF NOT EXISTS pages(id INTEGER NOT NULL PRIMARY KEY, title TEXT NOT NULL, redirect_to TEXT NULL)")
    con.exec(s"CREATE TABLE IF NOT EXISTS categories(id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, title TEXT NOT NULL UNIQUE)")
    con.exec("CREATE TABLE IF NOT EXISTS page_categories(page_id INTEGER NOT NULL REFERENCES pages(id) ON DELETE CASCADE, category_id INTEGER NOT NULL REFERENCES categories(id) ON DELETE CASCADE)")

    val existing = con.head("SELECT COUNT(*) FROM pages")(_.getInt(1))
    if(existing < size) {
      var count = 0L
      val prog = new Progress(dst.getName, 0, size)
      logger.info(s"start importing page categories to database: $dst")
      Source.fromFile(dst, "UTF-8").getLines().drop(1).grouped(1000).foreach { lines =>
        val items = lines.map(_.split("\t").toList match {
          case id :: title :: Nil => (id.toLong, sql(norm(title)), "", Nil)
          case id :: title :: red :: cats => (id.toLong, sql(norm(title)), sql(norm(red)), cats.map(sql))
          case _ => throw new IllegalStateException(s"unexpected line: ${lines.mkString("\n")}")
        })

        // ページタイトルとリダイレクト
        val pageValues = items.map { case (id, title, red, _) =>
          s"($id,'$title',${if(red.isEmpty) "NULL" else s"'$red'"})"
        }.mkString(",")
        con.exec(s"INSERT INTO pages(id, title, redirect_to) VALUES$pageValues")

        // カテゴリ
        val categories = items.flatMap(_._4).distinct.sorted
        val catIn = categories.mkString("'", "','", "'")
        val existing = con.query(s"SELECT id, title FROM categories WHERE title IN ($catIn)") { rs =>
          (rs.getString(2), rs.getLong(1))
        }.toMap
        val newbie = categories.filterNot(existing.contains).map { cat =>
          val catId = con.headOption("SELECT id FROM categories WHERE title=?", cat)(_.getLong(1)).getOrElse {
            con.exec("INSERT INTO categories(title) VALUES(?)", cat)
            con.head("SELECT id FROM categories WHERE title=?", cat)(_.getLong(1))
          }
          (cat, catId)
        }.toMap
        val categoryIds = existing ++ newbie
        val catValues = items.flatMap { case (id, _, _, cats) =>
          cats.map(categoryIds.apply).distinct.map(cid => s"($id,$cid)")
        }.mkString(",")
        if(catValues.nonEmpty) {
          con.exec(s"INSERT INTO page_categories(page_id, category_id) VALUES$catValues")
        }
        count += lines.length
        prog.report(count, items.head._2)
      }

      logger.info(f"finish to import page redirect and categories: $count%,d pages imported")
    } else {
      logger.info(f"skip to import page redirect and categories: $size%,d pages exists")
    }
  }

  /**
    * 指定された Extract 済み Wikipedia ファイル内の記事を取り込みます。ファイルは wikiextract を使用して抽出した
    * 記事のテキストが &lt;doc&gt; で囲まれた形式を想定しています。
    *
    * @param con  データベース接続
    * @param file wikiextract で抽出したファイル (AA/wiki_00, AA/wiki_01, ...)
    * @return データベースに保存した記事数
    */
  private[this] def importDocument(con:Connection, file:File):Int = {
    val ATTR = """([a-zA-Z0-9-_]*)\s*=\s*"(.*?)"""".r

    def attr(elem:String):Map[String, String] = {
      val a = scala.collection.mutable.HashMap[String, String]()
      val m = ATTR.pattern.matcher(elem)
      while(m.find()) {
        a += (m.group(1) -> m.group(2))
      }
      a.toMap
    }

    val buffer = new StringBuilder()
    var id:Option[Int] = None
    var title:Option[String] = None
    var lineNum = -1
    val values = try {
      Source.fromFile(file, "UTF-8").getLines.zipWithIndex.flatMap { case (line, num) =>
        lineNum = num
        line.trim() match {
          case begin if begin.startsWith("<doc") && Character.isWhitespace(begin.charAt(4)) =>
            // WSDL の通信サンプル <document> に反応する
            assert(id.isEmpty && title.isEmpty && buffer.toString.trim().isEmpty, s"$file(${num + 1}): $line ($id, $title, $buffer)")
            val a = attr(begin)
            id = a.get("id").map(_.toInt)
            title = a.get("title").map(_.replaceAll("[\\s\u3000]+", " ").trim())
            buffer.length = 0
            None
          case "</doc>" =>
            assert(id.nonEmpty && title.nonEmpty && buffer.nonEmpty, s"$file(${num + 1}): $line ($id, $title, $buffer)")
            val value = (id.get, title.get, buffer.toString())
            id = None
            title = None
            buffer.length = 0
            Some(value)
          case text =>
            if(buffer.nonEmpty) {
              buffer.append("\n")
            }
            buffer.append(text)
            None
        }
      }.map { case (i, t, content) =>
        s"($i,'${sql(t)}','${sql(content)}')"
      }.mkString(",")
    } catch {
      case ex:Throwable =>
        throw new Exception(s"$file($lineNum)", ex)
    }
    con.exec(s"INSERT INTO articles(id,title,content) VALUES$values")
  }

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
