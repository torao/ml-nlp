package at.hazm.ml.nlp.pipeline

import java.io.{BufferedReader, File}
import java.nio.charset.Charset
import java.sql.DriverManager

import at.hazm.ml.io.openTextInput

trait Source[OUT] extends Iterator[OUT] with AutoCloseable {

  def >>[T](next:IntakePipe[OUT, T]):IntakePipe[OUT, T] = {
    next._src = Some(this)
    next
  }
}

object Source {

  /**
    * 1行1データのテキストで表される Source です。
    *
    * @param in 入力ストリーム
    */
  class TextLine(in:BufferedReader) extends Source[String] {

    /**
      * 指定されたファイルから行データを読み込む Source を構築します。ファイルはプレーンテキストの他にその圧縮形式である
      * GZIP または BZIP2 を指定することができます。これらの圧縮形式はファイルの拡張子で認識されます。
      *
      * @param file    行データの含まれているテキストファイルまたはその圧縮ファイル
      * @param charset テキスト入力の文字セット。省略時はシステムデフォルト。
      */
    def this(file:File, charset:Charset = Charset.defaultCharset()) = this(openTextInput(file, charset))

    private[this] var line = in.readLine()

    override def close():Unit = in.close()

    override def hasNext:Boolean = line != null

    override def next():String = {
      val current = line
      line = in.readLine()
      current
    }
  }

  /**
    * データベースから JDBC 接続経由でデータを取得する Source です。JDBC ドライバは事前にロードされている必要があります。
    * このソースは構築時に指定されたデータベースに接続し SQL を実行してその最初のカラムをデータとして使用します。
    *
    * @param url      JDBC URL
    * @param username ユーザ名
    * @param password パスワード
    * @param query    データ取得のためのクエリー
    * @param args     クエリーのパラメータ
    */
  class Database(url:String, username:String, password:String, query:String, args:AnyVal*) extends Source[String] {
    private[this] val con = DriverManager.getConnection(url, username, password)
    private[this] val stmt = con.prepareStatement(query)
    private[this] val rs = {
      args.zipWithIndex.foreach { case (arg, i) => stmt.setObject(i + 1, arg) }
      stmt.executeQuery()
    }
    private[this] var _hasNext = rs.next()

    override def hasNext:Boolean = _hasNext

    override def next():String = {
      val value = rs.getString(1)
      _hasNext = rs.next()
      value
    }

    override def close():Unit = {
      rs.close()
      stmt.close()
      con.close()
    }
  }

}
