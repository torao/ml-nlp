package at.hazm.core.db

import java.io.File

import org.h2.Driver

/**
  * H2 Database を使用したローカルデータベース機能です。
  *
  * @param file データベースファイル
  */
class LocalDB(file:File) extends Database(s"jdbc:h2:${file.getAbsolutePath}", "", "", classOf[Driver].getName)