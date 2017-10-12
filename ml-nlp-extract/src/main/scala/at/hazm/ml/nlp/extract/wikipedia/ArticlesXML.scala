package at.hazm.ml.nlp.extract.wikipedia

import java.io._
import javax.xml.XMLConstants
import javax.xml.stream.{XMLInputFactory, XMLStreamReader}

import at.hazm.core.io.ProgressInputStream
import at.hazm.core.util.Diagnostics.Progress
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream

import scala.annotation.tailrec

/**
  * ダンプされた Wikipedia 記事の一つを表すクラスです。
  *
  * @param id      記事のページ ID
  * @param title   タイトル
  * @param content 内容 (Wiki書式)
  */
case class Article(id:Long, title:String, content:String)

object ArticlesXML {

  /**
    * 指定された Wikipedia ダンプファイルから記事を読み込む Iterator を参照します。列挙は最後まで読み込む必要があります。
    *
    * @param src 記事を列挙するファイル
    * @return 記事の列挙
    */
  def parse(src:File):Iterator[Article] = {
    val prog = new Progress(src.getName, 0, src.length())
    val in = new BZip2CompressorInputStream(new BufferedInputStream(new ProgressInputStream(src, { c:Long => prog.report(c) }), 10 * 1024))
    val factory = XMLInputFactory.newInstance()
    // factory.setProperty(XMLConstants.FEATURE_SECURE_PROCESSING, false)
    val reader = factory.createXMLStreamReader(in)
    new ArticleIterator(reader, in)
  }

  /**
    * XML ストリームから Wikipedia の記事部分を抽出して返す Iterator です。
    *
    * @param reader XML ストリームリーダー
    * @param in     終了時にクローズするストリーム
    */
  private class ArticleIterator(reader:XMLStreamReader, in:InputStream) extends Iterator[Article] {
    private[this] var article:Option[Article] = None

    override def hasNext:Boolean = {
      fill()
      article.isDefined
    }

    override def next():Article = {
      val x = article.get
      article = None
      x
    }

    private[this] def fill():Unit = if(article.isEmpty) {
      import javax.xml.stream.XMLStreamConstants._
      @tailrec
      def skipWhile(localName:String):Boolean = if(reader.hasNext) {
        if(reader.next() == START_ELEMENT && reader.getLocalName == localName) true else skipWhile(localName)
      } else false

      @tailrec
      def doUntil(localName:String)(f:(String) => Unit):Unit = if(reader.hasNext) {
        reader.next() match {
          case END_ELEMENT if reader.getLocalName == localName => None
          case START_ELEMENT =>
            f(reader.getLocalName)
            doUntil(localName)(f)
          case _ => doUntil(localName)(f)
        }
      } else None

      if(skipWhile("page")) {
        var id = -1L
        var title = ""
        var text = ""
        doUntil("page") {
          case "id" =>
            if(id < 0) id = reader.getElementText.trim().toLong
          case "title" => title = reader.getElementText.trim()
          case "text" => text = reader.getElementText.trim()
          case _ => None
        }
        article = Some(Article(id, title, text))
      } else {
        reader.close()
        in.close()
      }
    }
  }

}
