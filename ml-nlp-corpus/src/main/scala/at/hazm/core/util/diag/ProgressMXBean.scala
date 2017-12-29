/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements; and to You under the Apache License, Version 2.0.
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package at.hazm.core.util.diag

import javax.management.MXBean

@MXBean
trait ProgressMXBean {
  /** 開始時刻 */
  def getStart:String
  /** 終了予定時刻 */
  def getEnd:String
  /** このプロセスで処理した数 */
  def getCurrent:Long
  /** 全体で処理した数 */
  def getFinished:Long
  /** 全体の数 */
  def getTotal:Long
  /** 現在のメッセージ */
  def getMessage:String

  /** 処理の中断 */
  def shutdown():Unit
}
