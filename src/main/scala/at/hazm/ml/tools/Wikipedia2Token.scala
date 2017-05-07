package at.hazm.ml.tools

import java.io._
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import at.hazm.ml.io.using
import at.hazm.ml.nlp.{Token, normalize}

/**
  * コマンドラインで指定された Wikipedia テキストファイルを形態素に分解し文ごとに区切って出力します。
  *
  * Wikipedia2Token jawiki-20170401-pages-articles.tsv.gz [dest]
  *
  * [dest] を省略した場合は標準出力に出力します。
  */
object Wikipedia2Token extends App {

  val (src, dst) = args.toList match {
    case s :: d :: rest => (s, Some(d))
    case s :: Nil => (s, None)
    case Nil =>
      System.err.println(s"USAGE: ${getClass.getName.dropRight(1)} [jawiki-latest-pages-articles.tsv.gz] {output}")
      System.exit(1)
      throw new Exception()
  }

  def GZIPReader(file:String):BufferedReader = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(src)), "UTF-8"))
  def BufferedWriter(os:OutputStream):PrintWriter = new PrintWriter(new OutputStreamWriter(os, "UTF-8"))


  // 進捗表示のためファイル内の行数を取得
  System.err.print(s"$src: 記事数を数えています...")
  val lineCount = using(GZIPReader(src)) { in =>
    Iterator.continually(in.readLine).takeWhile(_ != null).size
  }
  System.err.println(f" 完了: $lineCount%,d")

  progress(new File(src).getName, lineCount){ prog =>
    var line = 0
    val os = dst.map{ file => new GZIPOutputStream(new FileOutputStream(file)) }
    val out = BufferedWriter(os.getOrElse(System.out))
    using(GZIPReader(src)) { in =>
      Iterator.continually(in.readLine).takeWhile(_ != null).map(_.split("\t", 3).toList).foreach {
        case id :: title :: content :: Nil =>
          if (!title.contains("曖昧さ回避")) {
            def _splitSentence(tokens:List[Token]):Seq[List[Token]] = {
              val i = tokens.indexWhere(_.pos == "記号-句点")
              if (i < 0) Seq(tokens) else {
                tokens.take(i + 1) +: _splitSentence(tokens.drop(i + 1))
              }
            }

            val norm = dropRuby(normalize(content))
            out.println(id + "\t" + _splitSentence(Token.parse(norm).toList).map { tokens =>
              tokens.map { token =>
                (token.base.getOrElse(token.term).trim(), token.pos.split("-").head)
              }.filter { token => token._1.nonEmpty }.map { case (term, pos) =>
                s"$term:$pos"
              }.mkString(" ")
            }.mkString("\t"))
          }
          line += 1
          prog(line, title)
        case unknown =>
          System.err.println(s"ERROR: ${unknown.mkString(" ")}")
      }
    }
    os.foreach{ _ => out.close() }
  }

  private[this] def dropRuby(text:String):String = {
    // text.replaceAll("""\([ぁ-んァ-ンー、/\s英略称語年月日紀元前男女性\-:;,0-9a-zA-Z・]*\)""", "")
    // Wikipedia の場合、かっこはフリガナや補足のことが多いため削除しても文章の意味は通じる
    text.replaceAll("""\(.*?\)""", "")
  }
}
