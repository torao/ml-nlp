/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements; and to You under the Apache License, Version 2.0.
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package at.hazm.core.db

import java.sql.SQLException

/**
  * RDBMS 依存の定義や処理など。
  */
trait RDBMS {
  val LONGVARCHAR:String
  val LONGVARBINARY:String
  def isDuplicateKey(ex:SQLException):Boolean
}

object RDBMS {

  val Default:RDBMS = PostgreSQL

  case object PostgreSQL extends RDBMS {
    val LONGVARCHAR:String = "TEXT"
    val LONGVARBINARY:String = "BYTEA"
    def isDuplicateKey(ex:SQLException):Boolean = ex.getSQLState == "23505"
  }

}
