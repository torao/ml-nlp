package at.hazm.ml.tools

import java.io.File
import java.nio.charset.StandardCharsets

import at.hazm.ml.io.{readLine, writeLine}
import at.hazm.ml.nlp.knowledge.{Knowledge, Source, Synonym, Wikipedia}
import at.hazm.ml.nlp.{Token, normalize, splitSentence}
import jline.console.ConsoleReader

import scala.collection.mutable

object Brain {

  def main(args:Array[String]):Unit = {
    val knowledge = new Knowledge(new File("brain.db"))
    val history = new File(".brain_history")

    // コンソールの構築と入力履歴の読み出し
    val console = new ConsoleReader()
    if(history.isFile) {
      val commands = readLine(history)(in => Iterator.continually(in.readLine()).takeWhile(_ != null).toList)
      (if(commands.length > 100) {
        writeLine(history) { out =>
          val c100 = commands.takeRight(100)
          c100.foreach(out.println)
          c100
        }
      } else {
        commands
      }).foreach { line => console.getHistory.add(line) }
    }

    Iterator
      .continually(console.readLine("brain> "))
      .takeWhile(line => line != null && line.trim.toLowerCase != "quit")
      .map { c =>
        writeLine(history, append = true)(_.println(c))
        c.split("\\s+").toList
      }.foreach {
      case "tokenize" :: sentence =>
        val tokens = Token.parse(normalize(sentence.mkString(" ")))
        val table = tokens.zipWithIndex.map { case (token, i) =>
          List((i + 1).toString, token.term, token.base.getOrElse(""), token.pos, token.inflection.getOrElse(""), token.reading.getOrElse(""))
        }
        if(table.nonEmpty) {
          def len(t:String):Int = t.getBytes("Shift_JIS").length

          val size = for(x <- table.head.indices) yield table.map(c => len(c(x))).max
          table.map(_.map { case "" => "-"; case x => x }).foreach { row =>
            def r(i:Int) = " " * (size(i) - len(row(i))) + row(i)

            def l(i:Int) = row(i) + " " * (size(i) - len(row(i)))

            System.out.println(s"  ${r(0)}. ${l(1)} ${l(2)} ${l(3)} ${l(4)} ${l(5)}")
          }
        }
      case "search" :: term =>
        knowledge.cyclopedia.search(normalize(term.mkString(" "))).sortBy(_.qualifier).foreach { term =>
          if(term.qualifier.isEmpty) {
            System.out.println(s"  ${term.meanings}")
          } else {
            System.out.println(s"    (${term.qualifier}) ${term.meanings}")
          }
        }
      case "synonym" :: terms =>
        val search = normalize(terms.mkString(" "))
        val entities = mutable.HashSet[String]()
        val aliases = mutable.Buffer[Synonym.Term]()
        knowledge.cyclopedia.search(search).sortBy(_.qualifier).foreach{ t =>
          entities.add(t.name)
          aliases.appendAll(knowledge.synonyms.reverseSearch(t.term))
        }
        knowledge.synonyms.search(search) match {
          case Seq() =>
            //System.out.println(search)
            aliases.appendAll(knowledge.synonyms.reverseSearch(search))
          case syns =>
            syns.sortBy(_.qualifier).foreach { term =>
              entities.add(term.alterName)
              aliases.appendAll(knowledge.synonyms.reverseSearch(term.alterTerm))
            }
        }
        entities.toSeq.sorted.foreach(System.out.println)
        aliases.groupBy(_.id).values.map(_.head).toSeq.sortBy(_.name)foreach{ t =>
          System.out.println(f"${t.id}%8d. ${t.name}")
        }
      case "import" :: fileType :: uri :: lang :: file :: Nil =>
        if(!lang.matches("[a-z]{2}(-[A-Z]{2})?")) {
          System.out.println(s"ERROR: invalid language: $lang")
        } else Source.get(uri) match {
          case Some(source) =>
            fileType match {
              case "cyclopedia" => importSource(knowledge, source, lang, new File(file))
              case "synonyms" => importSynonyms(knowledge, source, lang, new File(file))
            }
          case None =>
            System.out.println(s"ERROR: unsupported uri $uri")
        }
      case "help" :: _ =>
        System.out.println("tokenize [sentence]")
        System.out.println("import [uri] [lang] [file] ... import dictionary [file] to database on uri, uri='wikipedia'")
        System.out.println("search [term]")
      case "" :: Nil => ()
      case unknown =>
        System.out.println(s"ERROR: unknown command ${unknown.head}, please refer help")
    }
  }

  private[this] def importSource(knowledge:Knowledge, source:Source, lang:String, file:File):Unit = source match {
    case Wikipedia =>
      val Qualifier = """(.+)\s*\((.*)\)""".r
      progress(file.getName, countLines(file)) { prog =>
        readLine(file) { in =>
          val sourceId = knowledge.source.id(source)
          var currentLine = 0
          prog(currentLine, "begin")
          knowledge.cyclopedia.register(sourceId, lang) { callback =>
            Iterator.continually(in.readLine()).takeWhile(_ != null).foreach { line =>
              // [12][地理学][地理学  地理学（ちりがく、、、）は、空間ならびに自然と、経済・社会・文化等との関係を対象とする学問の分野。…]
              val docId :: title :: contents :: Nil = line.split("\t").toList
              val (term, qualifier) = normalize(title) match {
                case Qualifier(t, q) => (t.trim(), q)
                case t => (t, "")
              }

              optimizeWikipedia(term, qualifier, contents).foreach { case (t, q, c) =>
                val url = s"https://ja.wikipedia.org/?curid=${docId.trim()}"
                callback(t, q, c, Some(url))
              }
              currentLine += 1
              prog(currentLine, title)
            }
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
      progress(file, StandardCharsets.UTF_8) { line =>
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
