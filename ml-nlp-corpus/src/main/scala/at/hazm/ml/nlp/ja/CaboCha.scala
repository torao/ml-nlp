package at.hazm.ml.nlp.ja

import java.io._
import java.util.concurrent.atomic.AtomicInteger
import javax.xml.parsers.DocumentBuilderFactory

import at.hazm.ml.nlp.ja.CaboCha._
import at.hazm.ml.nlp.{Corpus, Morph, Paragraph}
import org.w3c.dom.{Element, Node, NodeList}
import org.xml.sax.InputSource

import scala.collection.mutable

/**
  * 係り受け解析を行うためのクラスです。外部コマンド {{cabocha}} をラップしています。
  *
  * @param cmd CaboCha の外部コマンド
  */
class CaboCha(cmd:String = "cabocha") extends AutoCloseable {
  private[this] val proc = Runtime.getRuntime.exec(Array(cmd, "-f", "3", "-n", "1"))
  private[this] val in = new BufferedReader(new InputStreamReader(proc.getInputStream, "Windows-31j"))
  private[this] val out = new PrintWriter(new OutputStreamWriter(proc.getOutputStream, "Windows-31j"))
  private[this] val err = new BufferedReader(new InputStreamReader(proc.getErrorStream, "Windows-31j"))

  def parse(id:Int, text:String, corpus:Corpus):Paragraph = {

    // 形態素解析/係り受け解析の実行
    val rawSentence = parse(text)
    val ss = CaboCha.splitSentence(rawSentence)

    // 文書内の形態素を登録して Key -> Index のマップを作製
    val morphs = ss.flatMap(_.chunks.flatMap(_.tokens.map(_.toMorph)))
    val idMap = corpus.synchronized {
      corpus.vocabulary.register(morphs).zip(morphs).map { case (i, m) => (m.key, i) }.toMap
    }

    // パラグラフの構築と登録
    val sentenceId = new AtomicInteger(0)
    val par = Paragraph(
      id = id,
      sentences = ss.map { sentence =>
        Paragraph.Sentence(
          id = sentenceId.getAndIncrement(),
          clauses = sentence.chunks.map { chunk =>
            Paragraph.Clause(
              id = chunk.id,
              link = chunk.link,
              rel = chunk.rel,
              score = chunk.score,
              head = chunk.head,
              func = chunk.func,
              morphs = chunk.tokens.map { token =>
                Paragraph.MorphIndex(
                  id = token.id,
                  morphId = idMap(token.toMorph.key),
                  ne = token.ne
                )
              }
            )
          }
        )
      }
    )
    corpus.synchronized {
      corpus.paragraphs.set(id, par)
    }
    par
  }

  def parse(text:String):CaboCha.Sentence = {
    val xml = "<?xml version=\"1.0\"?>\n" + this.synchronized {
      out.println(text.replaceAll("\\r?\\n", " "))
      out.flush()
      Iterator.continually(in.readLine()).takeWhile(line => line != null && line != "</sentence>").mkString("\n")
    } + "\n</sentence>"

    val factory = DocumentBuilderFactory.newInstance()
    val builder = factory.newDocumentBuilder()
    val doc = builder.parse(new InputSource(new StringReader(xml)))

    val root = doc.getDocumentElement
    val chunks = if(root.getTagName == "sentence"){
      root.getChildNodes.toElements.filter(_.getTagName == "chunk").map{ chunk =>
        val toks = chunk.getChildNodes.toElements.filter(_.getTagName == "tok")
        Chunk(
          id = chunk.getAttribute("id").toInt,
          link = chunk.getAttribute("link").toInt,
          rel = chunk.getAttribute("rel"),
          score = chunk.getAttribute("score").toDouble,
          head = chunk.getAttribute("head").toInt,
          func = chunk.getAttribute("func").toInt,
          tokens = toks.map { tok =>
            CaboCha.Token(
              id = tok.getAttribute("id").toInt,
              term = tok.getTextContent,
              feature = tok.getAttribute("feature"),
              ne = tok.getAttribute("ne"))
          }
        )
      }
    } else Seq.empty
    Sentence(chunks)
  }

  def close():Unit = synchronized {
    out.close()
    proc.waitFor()
  }

}

object CaboCha {

  case class Sentence(chunks:Seq[Chunk])

  case class Chunk(id:Int, link:Int, rel:String, score:Double, head:Int, func:Int, tokens:Seq[CaboCha.Token]) {
    def headToken:Token = tokens.find { t => t.id == head }.get

    def funcToken:Token = tokens.find { t => t.id == func }.get
  }

  case class Token(id:Int, term:String, feature:String, ne:String) {
    lazy val (pos1, pos2, pos3, pos4, conjugationType, conjugationForm, baseForm, reading, pronunciation) = feature.split(",") match {
      case Array(p1, p2, p3, p4, ct, cf, bf, r, p) =>
        (p1, p2, p3, p4, ct, cf, bf, r, p)
      case Array(p1, p2, p3, p4, ct, cf, bf) =>
        (p1, p2, p3, p4, ct, cf, bf, "*", "*")
    }

    def toMorph:Morph = {
      def a(text:String):String = if(text == "*") "" else text

      Morph(
        surface = term, pos1 = a(pos1), pos2 = a(pos2), pos3 = a(pos3), pos4 = a(pos4),
        conjugationType = a(conjugationType), conjugationForm = a(conjugationForm),
        baseForm = a(baseForm), reading = a(reading), pronunciation = a(pronunciation)
      )
    }

    def toToken:at.hazm.ml.nlp.Token = {
      val Array(pos1, pos2, pos3, pos4, conjugationType, conjugationForm, baseForm, reading, pronunciation) = feature.split(",")
      /** 品詞 タイプ1 */
      /** 品詞 タイプ2 */
      /** 品詞 タイプ3 */
      /** 品詞 タイプ4 */
      /** 活用型 */
      /** 活用形 */
      /** 基本型 */
      /** 読み */
      /** 発音 */

      at.hazm.ml.nlp.Token(term, s"$pos1-$pos2-$pos3-$pos4".replaceAll("\\-\\*", ""),
        if(baseForm == "*") None else Some(baseForm),
        if(conjugationType == "*") None else Some(conjugationType),
        if(reading == "*") None else Some(reading))
    }
  }

  /**
    * 文節のかかり先や主辞などの情報が不正確になります。この文分割処理の後に文節の正確な情報が必要な場合は、形態素を再度連結して係り受け
    * 解析を行う必要があります。
    *
    * @param ss 係り受け解析結果
    * @return 文に分割された文節と形態素
    */
  def splitSentence(ss:Sentence):Seq[Sentence] = {
    val sentences = mutable.Buffer[CaboCha.Sentence]()
    val chunks = mutable.Buffer[CaboCha.Chunk]()
    ss.chunks.filter(_.tokens.nonEmpty).foreach { chunk =>
      val lastTokenIndex = findEOS(chunk)
      if(lastTokenIndex < 0) {
        chunks.append(chunk)
      } else if(lastTokenIndex == chunk.tokens.length - 1) {
        sentences.append(CaboCha.Sentence(chunks.toList :+ chunk))
        chunks.clear()
      } else {
        val last = chunk.copy(link = -1, tokens = chunk.tokens.take(lastTokenIndex))
        val nextHead = CaboCha.Chunk(-1, chunk.link, "", 0.0, -1, -1, chunk.tokens.drop(lastTokenIndex))
        sentences.append(CaboCha.Sentence(chunks.toList :+ last))
        chunks.clear()
        chunks.append(nextHead)
      }
    }
    if(chunks.nonEmpty) {
      sentences.append(CaboCha.Sentence(chunks.toList))
    }
    sentences.toList
  }

  /**
    * 係り受けの関係を解析して指定された文を最短文に変換します。
    *
    * @param ss 解析済みの文
    * @return 最短文
    */
  def splitSentenceByRelation(ss:CaboCha.Sentence):Seq[CaboCha.Sentence] = {
    def getSequence(c:Chunk, buffer:mutable.Buffer[Chunk]):Seq[Chunk] = {
      buffer.append(c)
      if(c.link < 0) buffer else ss.chunks.find(_.id == c.link) match {
        case Some(next) => getSequence(next, buffer)
        case None => buffer
      }
    }

    val linkedChunkIds = ss.chunks.map(_.link).toSet
    ss.chunks.filter(c => !linkedChunkIds.contains(c.id)).map { leaf =>
      CaboCha.Sentence(getSequence(leaf, mutable.Buffer()))
    }
  }

  /**
    * 指定された文節が文の区切りを含んでいる場合にその最後のトークンを示す位置を返します。文節が文区切りを含まない場合は
    * 府の値を返します (CaboCha による分節の誤認識をルールで補完する処理です)。以下のトークンを文の終わりとして使用します。
    *
    * 1. 句点記号
    * 2. 終助詞
    *
    * 分節中に文の終りを示すトークンが複数含む場合は最初のトークンの位置のみを返します。
    *
    * @param chunk 文節
    * @return 文の区切りのトークンの位置
    */
  private[this] def findEOS(chunk:Chunk):Int = chunk.tokens.indices.find { i =>
    val token = chunk.tokens(i)
    (token.pos1 == "記号" && token.pos2 == "句点") || (token.pos1 == "助詞" && token.pos2.contains("終助詞"))
  }.getOrElse(-1)

  //  def simplify(text:String):Unit = {
  //    val cabocha = new CaboCha()
  //    Source.fromInputStream(System.in).getLines.map(_.trim()).filter(_.nonEmpty).foreach { line =>
  //      val sentence = cabocha.parse(line)
  //      // どこからもかかっていない chunk を取得
  //      val linkedChunkIds = sentence.chunks.map(_.link).toSet
  //      sentence.chunks.filter(c => !linkedChunkIds.contains(c.id)).foreach { leaf =>
  //        def getSequence(c:Chunk):Seq[Chunk] = {
  //          if(c.link < 0) Seq(c) else {
  //            c +: getSequence(sentence.chunks.find(_.id == c.link).get)
  //          }
  //        }
  //
  //        System.out.println(getSequence(leaf).map(_.tokens.map(_.term).mkString(" ")).mkString("[", "][", "]"))
  //      }
  //    }
  //    cabocha.close()
  //  }

  private[ja] implicit class _NodeList(nl:NodeList){
    def asScala:List[Node] = (for(i <- 0 until nl.getLength) yield nl.item(i)).toList
    def toElements:List[Element] = asScala.collect{ case e:Element => e }
  }

}