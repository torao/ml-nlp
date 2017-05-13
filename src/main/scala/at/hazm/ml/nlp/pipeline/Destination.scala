package at.hazm.ml.nlp.pipeline

import java.io.{File, Writer}
import java.nio.charset.Charset
import java.sql.DriverManager

import at.hazm.ml.io.openTextOutput

trait Destination[OUT] extends AutoCloseable {

  def <<[T](prev:ExhaustPipe[T,OUT]):ExhaustPipe[T,OUT] = {
    prev._dest = Some(this)
    prev
  }
  def write(data:OUT)
}


object Destination {

  /**
    * 1行1データのテキストで表される Destination です。
    *
    * @param out 入力ストリーム
    */
  case class TextLine(out:Writer) extends Destination[String] {

    /**
      * 指定されたファイルへ行データを書き込む Destination を構築します。ファイルはプレーンテキストの他にその圧縮形式である
      * GZIP または BZIP2 を指定することができます。これらの圧縮形式はファイルの拡張子で認識されます。
      *
      * @param file    行データを書き込むテキストファイルまたはその圧縮ファイル
      * @param charset テキスト出力の文字セット。省略時はシステムデフォルト。
      */
    def this(file:File, charset:Charset = Charset.defaultCharset()) = this(openTextOutput(file, charset))

    override def write(data:String):Unit = {
      out.write(data)
      out.write('\n')
      out.flush()
    }

    override def close():Unit = out.close()
  }

  /**
    * JDBC 経由でデータベースにデータを保存する Destination です。JDBC ドライバは事前にロードされている必要があります。
    * このソースは構築時に指定されたデータベースに接続し SQL を実行して insert ステートメントを実行します。
    *
    * @param url      JDBC URL
    * @param username ユーザ名
    * @param password パスワード
    * @param insert   データ取得のためのクエリー
    */
  case class Database(url:String, username:String, password:String, insert:String) extends Destination[String] {
    private[this] val con = DriverManager.getConnection(url, username, password)
    private[this] val stmt = con.prepareStatement(insert)

    override def write(data:String):Unit = synchronized {
      stmt.setString(1, data)
      stmt.executeUpdate()
    }

    override def close():Unit = {
      stmt.close()
      con.close()
    }
  }

}
