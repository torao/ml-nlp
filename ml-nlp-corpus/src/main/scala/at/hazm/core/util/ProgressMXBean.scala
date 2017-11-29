package at.hazm.core.util

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
