package at.hazm.core.db

import java.io.File
import java.sql.{Connection, DriverManager}

/**
  * H2 Database を使用したローカルデータベース機能です。
  *
  * @param file データベースファイル
  */
class LocalDB(file:File) extends Database(s"jdbc:h2:$file", "", ""){

  /**
    * 新しいデータベース接続を作成します。
    *
    * @return
    */
  override def newConnection:Connection = DriverManager.getConnection(url, username, password)
}