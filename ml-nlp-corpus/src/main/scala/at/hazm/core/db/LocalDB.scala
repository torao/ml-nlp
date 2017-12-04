package at.hazm.core.db

import java.io.File

import org.h2.Driver
import org.slf4j.LoggerFactory

/**
  * H2 Database を使用したローカルデータベース機能です。
  *
  * @param file データベースファイル
  */
class LocalDB(val file:File, readOnly:Boolean = false) extends Database({
  val real = new File(file.getAbsoluteFile.getParentFile, file.getName + ".h2.mv.db")
  if(!real.exists()) {
    LocalDB.logger.info(s"creating new local database: ${real.getAbsolutePath}")
  }
  s"jdbc:h2:${file.getAbsolutePath}.h2"
}, "", "", classOf[Driver].getName, readOnly)

object LocalDB {
  private[LocalDB] val logger = LoggerFactory.getLogger(classOf[LocalDB])
}