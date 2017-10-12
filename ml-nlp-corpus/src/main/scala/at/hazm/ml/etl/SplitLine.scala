package at.hazm.ml.etl

import java.io.EOFException

import scala.annotation.tailrec
import scala.collection.mutable

/**
  * 任意の文字列列挙を行で分割する Transform です。
  */
class SplitLine extends Transform[String, String] {

  /**
    * 読み込み済みの文字列バッファ。前回の行分割評価での残った文字列が保存される。
    */
  private[this] val remains = new StringBuilder()

  /**
    * 解析済みの行が保存される。
    */
  private[this] val lines = mutable.Buffer[String]()

  override def reset():Unit = {
    lines.clear()
    remains.setLength(0)
  }

  override def hasNext:Boolean = lines.nonEmpty || remains.nonEmpty || source.hasNext

  override def next():String = {
    if(lines.isEmpty) {
      readLine()
      if(lines.isEmpty) {
        assert(remains.isEmpty)
        throw new EOFException()
      }
    }
    lines.remove(0)
  }

  /**
    * 有効な改行を検知するまで下層のパイプから文字列を読み込みます。
    */
  @tailrec
  private[this] def readLine():Unit = if(source.hasNext) {
    // 次の文字列を読み込み
    val text = source.next()
    remains.appendAll(text.toString)
    // 読み込み済みの文字列を行に分割
    // CR LF に対応するために EOF を検知するまでは最後の1文字は評価しない
    var head = 0
    var i = 0
    while(i < remains.length - 1) {
      if(remains.charAt(i) == '\n' || remains.charAt(i) == '\r') {
        lines.append(remains.substring(head, i))
        val skip = if(remains.charAt(i) == '\r' && remains.charAt(i + 1) == '\n') 2 else 1
        head = i + skip
        i = head
      } else i += 1
    }
    if(head > 0) {
      // 行に分解した部分を削除して終了
      remains.delete(0, head)
    } else {
      // 行を検出できていなければ再実行
      readLine()
    }
  } else if(remains.nonEmpty) {
    if(remains.lastOption.exists(c => c == '\n' || c == '\r')) {
      remains.deleteCharAt(remains.length - 1)
      lines.append(remains.toString())
    } else if(remains.nonEmpty) {
      lines.append(remains.toString())
    }
    remains.setLength(0)
  }
}