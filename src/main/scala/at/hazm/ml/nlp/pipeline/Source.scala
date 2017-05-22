package at.hazm.ml.nlp.pipeline

import java.io.{BufferedReader, File}
import java.nio.charset.Charset
import java.sql.DriverManager

import at.hazm.core.io.openTextInput

/**
  * データの入力元を表します。パイプライン上のデータソースは PULL 型の取り出しです。
  *
  * @tparam OUT このデータソースから取り出すデータの型
  */
trait Source[OUT] extends Iterator[OUT] with AutoCloseable {

  def >>[T](next:IntakeFilter[OUT, T]):Source[T] = new Source.Joint(this, next)

  def >>[T](next:(OUT) => T):Source[T] = new Source.Joint(this, next)

  /**
    * このパイプから次のデータが取り出せるかを判定します。このメソッドが true を返した場合、[[next()]] は例外なしで (システム由来で
    * 予測が不可能な例外を除く) データを返す必要があります。
    *
    * @return
    */
  def hasNext:Boolean

  /**
    * パイプから次のデータを取り出します。[[hasNext]] が false を返している状態でこのメソッドを呼び出すことはできません。
    *
    * @return 次のデータ
    * @throws NoSuchElementException [[hasNext]] が false を返しておりこれ以上データが存在しない場合
    */
  @throws[NoSuchElementException]
  def next():OUT

  /**
    * このソースの入力を初期位置に戻します
    */
  def reset():Unit

  /**
    * このパイプおよびその入力方向のソースをクローズしリソースを開放します。
    */
  def close():Unit

}

object Source {

  private[pipeline] class Joint[IN, OUT](src:Source[IN], filter:IntakeFilter[IN, OUT]) extends Source[OUT] {
    def this(src:Source[IN], filter:(IN) => OUT) = this(src, IntakeFilter(filter))

    override def hasNext:Boolean = src.hasNext || filter.hasNext

    /**
      * パイプから次のデータを取り出します。[[hasNext]] が false を返している状態でこのメソッドを呼び出すことはできません。
      *
      * @throws NoSuchElementException [[hasNext]] が false を返しておりこれ以上データが存在しない場合
      */
    @throws[NoSuchElementException]
    override def next():OUT = if(filter.hasNext) {
      filter.next()
    } else if(src.hasNext) {
      filter.forward(src.next())
    } else throw new NoSuchElementException("source has not more elements")

    override def reset():Unit = {
      filter.reset()
      src.reset()
    }

    override def close():Unit = {
      filter.close()
      src.close()
    }
  }

  /**
    * 指定されたファイルから行データを読み込む Source を構築します。ファイルはプレーンテキストの他にその圧縮形式である
    * GZIP または BZIP2 を指定することができます。これらの圧縮形式はファイルの拡張子で認識されます。
    *
    * @param file    行データの含まれているテキストファイルまたはその圧縮ファイル
    * @param charset テキスト入力の文字セット。省略時はシステムデフォルト。
    */
  class TextLine(file:File, charset:Charset = Charset.defaultCharset()) extends Source[String] {

    private[this] var in = openTextInput(file, charset)

    private[this] var line = in.readLine()

    override def reset():Unit = {
      in.close()
      in = openTextInput(file, charset)
    }

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
    private[this] var rs = {
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

    override def reset():Unit = {
      rs.close()
      rs = stmt.executeQuery()
    }

    override def close():Unit = {
      rs.close()
      stmt.close()
      con.close()
    }
  }

}
