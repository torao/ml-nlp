package at.hazm.core.db

import java.io.File

/**
  * SQLite3 を使用したローカルデータベース機能です。
  *
  * @param file データベースファイル
  */
class LegacyLocalDB(file:File) extends Database(s"jdbc:sqlite:$file", "", "")