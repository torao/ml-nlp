package at.hazm.ml.tools.cmd

import java.io.File
import java.nio.charset.StandardCharsets

import at.hazm.ml.io.readText
import at.hazm.ml.nlp.knowledge.{Knowledge, Source, Wikipedia}
import at.hazm.ml.nlp.{Token, normalize, splitSentence}
import at.hazm.ml.tools.{countLines, fileProgress}
import jline.console.ConsoleReader

import scala.collection.mutable

case class Import(key:String, knowledge:Knowledge) extends Command {

  val help:String =
    """[file] [source_uri] [lang] to (cyclopedia|synonym)
      |
    """.stripMargin

  def exec(console:ConsoleReader, args:List[String]):Unit = args match {
    case file :: _ if !new File(file).isFile =>
      throw CommandError(s"ファイル $file が存在しません")
    case _ :: sourceUri :: _ if Source.get(sourceUri).isEmpty =>
      throw CommandError(s"データソース $sourceUri は定義されていません")
    case _ :: _ :: lang :: _ if !lang.matches("[a-z]{2}(-[A-Z]{2})?") =>
      throw CommandError(s"言語コード $lang が不正です")
    case file :: sourceUri :: lang :: "to" :: (database@("cyclopedia" | "synonyms")) :: Nil =>
      val source = Source.get(sourceUri).get
      (database match {
        case "cyclopedia" => importCyclopedia _
        case "synonyms" => importSynonyms _
      }).apply(knowledge, source, lang, new File(file))
    case fileType :: _ if fileType != "cyclopedia" && fileType != "synonyms" =>
      throw CommandError(s"不明な")
  }


  private[this] def importCyclopedia(knowledge:Knowledge, source:Source, lang:String, file:File):Unit = source match {
    case Wikipedia =>
      val Qualifier = """(.+)\s*\((.*)\)""".r
      val sourceId = knowledge.source.id(source)
      knowledge.cyclopedia.register(sourceId, lang) { callback =>
        fileProgress(file, StandardCharsets.UTF_8, knowledge.db) { line =>
          // [12][地理学][地理学  地理学（ちりがく、、、）は、空間ならびに自然と、経済・社会・文化等との関係を対象とする学問の分野。…]
          val docId :: title :: contents :: Nil = line.split("\t").toList
          val (term, qualifier) = normalize(title) match {
            case Qualifier(t, q) => (t.trim(), q)
            case t => (t, "")
          }

          optimizeWikipedia(term, qualifier, contents).foreach { case (t, q, c) =>
            val url = s"https://${lang.take(2)}.wikipedia.org/?curid=${docId.trim()}"
            callback(t, q, c, Some(url))
          }
        }
      }
  }

  /**
    * 指定されたシノニムをデータベースにインポートします。ファイルは行ごとに [term1]{:[qualifier1]}[TAB][term2]{:[qualifier2]}
    * のフォーマットが認識されます。
    *
    * @param knowledge 知識データベース
    * @param source    情報源
    * @param lang      言語
    * @param file      インポートするファイル
    */
  private[this] def importSynonyms(knowledge:Knowledge, source:Source, lang:String, file:File):Unit = {

    def split(t:String):(String, String) = normalize(t).split(":", 2) match {
      case Array(w, q) => (w, q)
      case Array(w) => (w, "")
    }

    val sourceId = knowledge.source.id(source)
    knowledge.synonyms.register(sourceId, lang) { callback =>
      fileProgress(file, StandardCharsets.UTF_8) { line =>
        // [term1]{:[qualifier1]}[TAB][term2]{:[qualifier2]}
        val term :: alter :: Nil = line.split("\t").toList
        val (t1, q1) = split(term)
        val (t2, q2) = split(alter)
        callback(t1, q1, t2, q1)
      }
    }
  }

  private[this] def optimizeWikipedia(term:String, qualifier:String, content:String):Option[(String, String, String)] = {
    if(qualifier == "曖昧さ回避") {
      None
    } else if(term.endsWith("一覧")) {
      None
    } else splitSentence(content).toList match {
      case List(head, _*) =>
        // 文章の先頭には単語と同じ見出しが記述されているので除去
        def deleteHeading(term:String, content:String):String = {
          val normContent = normalize(content)
          if(normContent.startsWith(term) && normContent.length > term.length && Character.isWhitespace(normContent(term.length))) {
            normContent.substring(term.length).trim()
          } else normContent
        }

        def deleteDokuten(tokens:Seq[Token]):Seq[Token] = {
          def noun(t:Token) = t.pos.startsWith("名詞-") || t.pos == "記号-括弧閉"

          val buf = mutable.Buffer[Token]()
          for(i <- tokens.indices) {
            // [名詞]は、[名詞]で、[名詞]には、[名詞]では、それぞれの読点を削除
            if(tokens(i).pos == "記号-読点" && i - 2 >= 0 && tokens(i - 1).pos == "助詞-係助詞" && (
              noun(tokens(i - 2)) ||
                (tokens(i - 2).pos.startsWith("助詞-格助詞-") && i - 3 >= 0 && noun(tokens(i - 3)))
              )) {
              /* */
            } else buf.append(tokens(i))
          }
          buf
        }

        val tokens = Token.parse(Wikipedia.deleteParenthesis(deleteHeading(term, head)))
        val reading = deleteDokuten(tokens).map(_.term).mkString
        Some((term, qualifier, reading))
      case Nil => None
    }
  }

}
