package at.hazm.ml.etl

import java.io.{InputStreamReader, Reader}
import java.net.URL
import java.nio.charset.Charset

/**
  * ファイルからテキストを参照する事のできる Source です。ファイル内のテキストを読み込みますが、一度の呼び出しで取り
  * 出せる文字列の長さは不確定です。
  *
  * @param url        読み込むURL
  * @param charset    ファイルの文字セット
  * @param bufferSize 読み込みバッファサイズ
  */
class URLSource(url:URL, charset:Charset = Charset.defaultCharset(), bufferSize:Int = 4 * 1024) extends StreamSource(bufferSize) {
  override protected def reopen():Reader = new InputStreamReader(url.openStream(), charset)
}
