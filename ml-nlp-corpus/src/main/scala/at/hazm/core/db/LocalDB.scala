package at.hazm.core.db

import java.io.File

import org.h2.Driver
import org.slf4j.LoggerFactory

/**
  * H2 Database を使用したローカルデータベース機能です。
  *
  * @param file データベースファイル
  */
class LocalDB(file:File, readOnly:Boolean = false) extends Database({
  if(!file.exists()) LocalDB.logger.info(s"creating new local database: ${file.getAbsolutePath}")
  s"jdbc:h2:${file.getAbsolutePath}"
}, "", "", classOf[Driver].getName, readOnly)

object LocalDB {
  private[LocalDB] val logger = LoggerFactory.getLogger(classOf[LocalDB])
}