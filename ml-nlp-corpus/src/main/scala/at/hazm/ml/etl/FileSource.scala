package at.hazm.ml.etl

import java.io._
import java.nio.charset.Charset
import java.util.zip.GZIPInputStream

/**
  * ファイルからテキストを参照する事のできる Source です。ファイル内のテキストを読み込みますが、一度の呼び出しで取り
  * 出せる文字列の長さは不確定です。
  *
  * @param file       読み込むファイル
  * @param charset    ファイルの文字セット
  * @param bufferSize 読み込みバッファサイズ
  * @param gzip       ファイルが GZIP 圧縮されている場合 true
  */
class FileSource(file:File, charset:Charset = Charset.defaultCharset(), bufferSize:Int = 4 * 1024, gzip:Boolean = false) extends StreamSource(bufferSize) {
  override protected def reopen():Reader = {
    val is = new FileInputStream(file)
    new InputStreamReader(if(gzip) new GZIPInputStream(is) else is, charset)
  }
}
