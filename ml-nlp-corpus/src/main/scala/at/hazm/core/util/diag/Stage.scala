/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements; and to You under the Apache License, Version 2.0.
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package at.hazm.core.util.diag

import java.lang.management.ManagementFactory
import java.text.DateFormat
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import java.util.{Timer, TimerTask}
import javax.management.ObjectName

import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
  * ある処理ステージの進捗状態を保持するクラスです。
  *
  * @param prefix   このステージの名前
  * @param min      このステージのステップ数の最小値
  * @param max      このステージのステップ数の最大値
  * @param interval このステージでの進捗報告の間隔
  */
class Stage(prefix:String, min:Long, max:Long, interval:Long = 60 * 1000L) {

  import Stage._

  /** 処理開始時刻 */
  private[this] var startTimestamp:Long = -1

  private[this] val _current = new AtomicLong(min)
  private[this] var _message = ""
  private[this] var _timestamp = -1L

  /** 時刻とその時点での完了数を示す履歴配列 */
  private[this] val history:Array[(Long, Long)] = (0 until (10 * 60 * 1000L / interval).toInt).map(_ => (0L, 0L)).toArray

  /** このステージに中断命令が出されているかの制御フラグ */
  private[this] val _broken = new AtomicBoolean(false)

  /** このステージが終了しているかの制御フラグ */
  private[this] val _stopped = new AtomicBoolean(true)

  /** このステージが終了しているかを判定します。 */
  def stopped:Boolean = _stopped.get

  /** 進捗状況のレポート先 */
  private[this] val output = mutable.Buffer[Output]()

  /**
    * このステージの進捗状況のレポート先を追加します。
    *
    * @param output 進捗のレポート先
    */
  def addOutput(output:Output):Unit = this.output.append(output)

  /**
    * このステージから指定されたレポート先を削除します。
    *
    * @param output 削除するレポート先
    */
  def removeOutput(output:Output):Unit = {
    val i = this.output.indexOf(output)
    if(i >= 0) this.output.remove(i)
  }

  /** 進捗状況を定期的にレポートするためのタスク */
  private[this] var task:TimerTask = _

  private[this] val mxBeanName = new ObjectName(s"at.hazm.diag:name=$prefix")

  def start():Unit = if(_stopped.compareAndSet(true, false)) {
    _first = true

    task = new TimerTask {
      override def run():Unit = flush()
    }
    Stage.timer.scheduleAtFixedRate(task, interval, interval)
    this.startTimestamp = System.currentTimeMillis()
    this._timestamp = startTimestamp

    val server = ManagementFactory.getPlatformMBeanServer
    val mbean = new MXBean()
    server.registerMBean(mbean, mxBeanName)
  }

  def stop():Unit = if(_stopped.compareAndSet(false, true)) {
    task.cancel()
    val server = ManagementFactory.getPlatformMBeanServer
    server.unregisterMBean(mxBeanName)
    task.cancel()
    task = null
  }

  /**
    * 現在の進捗値を参照します。
    */
  def current:Long = _current.get()

  /**
    * 処理済みのステップ数を 1 つ加算します。
    *
    * @throws BreakDirective 外部から中断命令を受けている時
    */
  @throws(classOf[BreakDirective])
  def incrementStep():Unit = incrementStepWith("")

  /**
    * 指定された状況メッセージを付けて処理済みのステップ数を 1 つ加算します。
    *
    * @param message 状況メッセージ
    * @throws BreakDirective 外部から中断命令を受けている時
    */
  @throws(classOf[BreakDirective])
  def incrementStepWith(message:String):Unit = synchronized(setStepWith(_current.incrementAndGet(), message))

  /**
    * 処理済みのステップ数を指定された数に設定します。
    *
    * @param current 処理済みのステップ数 (min 以上 max 以下)
    * @throws BreakDirective 外部から中断命令を受けている時
    */
  @throws(classOf[BreakDirective])
  def setStepWith(current:Long):Unit = setStepWith(current, "")

  /**
    * 指定された状況メッセージを付けて処理済みのステップ数を指定された数に設定します。
    *
    * @param current 進捗
    * @param message 状況メッセージ
    * @throws BreakDirective 外部から中断命令を受けている時
    */
  @throws(classOf[BreakDirective])
  def setStepWith(current:Long, message:String):Unit = if(_broken.get()) {
    logger.warn(f"iteration is interrupted by user: $current%,d for $max%,d")
    throw new BreakDirective()
  } else {
    if(current < min) {
      logger.warn(s"current value less than minimum state: $current < $min: $message")
    } else if(current > max) {
      logger.warn(s"current value larger than maximum state: $current > $max: $message")
    } else if(current < this._current.get()) {
      logger.warn(s"current value less than progressing state: $current < ${this._current}: $message")
    }
    val firstStep = this.current == min
    this._current.set(current)
    this._message = message
    this._timestamp = System.currentTimeMillis()
    if(current == max) {
      stop()
      flush()
    } else if(firstStep) {
      flush()
    } else if(_stopped.get()) {
      logger.warn(f"iteration finished: $current%,d for $max%,d")
      throw new BreakDirective()
    }
  }

  /**
    * このステージの進捗を下層の Output へレポートします。
    */
  private[this] def flush():Unit = if(output.nonEmpty) {
    val time = if(current == max) {
      s"所要時間 ${intervalString(_timestamp - startTimestamp)}"
    } else {
      // 履歴に進捗を追加
      val t = history.synchronized {
        System.arraycopy(history, 0, history, 1, history.length - 1)
        history(0) = (_timestamp, current)
        estimateFinishTime(history, max)
      }

      if(t < _timestamp) {
        s"終了予想時間計測中"
      } else {
        s"残り ${intervalString(t - _timestamp)}"
      }
    }
    val msg = time + (if(_message.isEmpty) "" else s": ${_message}")
    // name:String, value:Long, min:Long, max:Long, msg:String
    val step = Step(prefix, current, min, max, msg, _first, current == max)
    output.foreach(_.print(step))
    _first = false
  }

  private[this] var _first = true

  /**
    * JMX 経由でこのステージの進捗状況を参照するための MXBean です。
    */
  private[this] class MXBean extends ProgressMXBean {
    override def getStart:String = format(startTimestamp)

    override def getEnd:String = format(estimateFinishTime(history, max))

    override def getCurrent:Long = current

    override def getFinished:Long = current

    override def getTotal:Long = max

    override def getMessage:String = _message

    override def shutdown():Unit = if(_stopped.compareAndSet(false, true)) {
      logger.info("shutting down")
    }

    private[this] def format(tm:Long):String = {
      DateFormat.getDateTimeInstance(DateFormat.MEDIUM, DateFormat.SHORT).format(tm)
    }
  }

}

object Stage {
  private[Stage] val logger = LoggerFactory.getLogger(classOf[Stage])

  /** 定期的にステージの進捗状況を出力するためのタイマー */
  private[Stage] val timer = new Timer(true)

  /** ステージの完了済みステップ数を表すクラス */
  case class Step(name:String, value:Long, min:Long, max:Long, msg:String, first:Boolean, last:Boolean) {

    /** このステップの進捗を％で表します */
    def percent:Double = (value - min) / (max - min).toDouble

    /** このステップを文字列として参照します */
    override def toString:String = f"$name: ${value - min}%,d / ${max - min}%,d (${(value - min) * 100.0 / (max - min)}%.1f%%) $msg"
  }

  trait Output {
    def print(step:Step):Unit
  }

  /**
    * 進捗状況を通常のログとして出力します。
    */
  case object Logger extends Output {
    def print(step:Step):Unit = logger.info(step.toString)
  }

  /**
    * 進捗状況を1行の進捗バー風に出力します。
    */
  case object ConsoleLine extends Output {
    private[this] val CHARS = ('A' to 'Z').mkString
    private[this] var cur = 0
    def print(step:Step):Unit = {
      if(step.first){
        System.out.print(s"${step.name}: ")
      }
      val next = ((step.value - step.min) / (step.max - step.min).toDouble * CHARS.length).toInt
      if(cur < next){
        for(i <- cur until next){
          System.out.write(CHARS(i))
        }
      }
      cur = next
      if(step.last){
        System.out.println()
        System.out.println(step.toString)
      }
      System.out.flush()
    }
  }

  def exec[T](prefix:String, min:Long, max:Long, interval:Long = 60 * 1000L)(f:(Stage) => T):T = {
    val stage = new Stage(prefix, min, max, interval)
    try {
      stage.start()
      f(stage)
    } catch {
      case ex:BreakDirective =>
        logger.warn("break directive detected", ex)
        System.exit(1)
        throw new Error()
    } finally {
      stage.stop()
    }
  }

  /**
    * 最小二乗法を用いて終了時間を予測する。計測できない場合は負の値を返す。
    *
    * @param history (時刻, 完了数) の履歴
    * @param maxP    完了数の取りうる最大値
    * @return 終了予想時刻 (UTC millis)
    */
  private[Stage] def estimateFinishTime(history:Array[(Long, Long)], maxP:Long):Long = {
    val (ts, ps) = locally {
      val hs = history.filter(_ != null)
      (hs.map(_._1), hs.map(_._2))
    }
    val n = ts.length
    val s = ts.zip(ps).map { case (t, p) =>
      (t.toDouble * p, t.toDouble, p.toDouble, t.toDouble * t)
    }.foldLeft((0.0, 0.0, 0.0, 0.0)) { case (ss, sb) =>
      (ss._1 + sb._1, ss._2 + sb._2, ss._3 + sb._3, ss._4 + sb._4)
    }
    val (sxy, sx, sy, sx2) = s
    val a = (n * sxy - sx * sy) / (n * sx2 - sx * sx)
    val b = (sx2 * sy - sxy * sx) / (n * sx2 - sx * sx)
    if(a.isInfinity || b.isInfinite) -1 else ((maxP - b) / a).toLong
  }

  /**
    * 指定された時刻を人が読める文字列に変換します。
    *
    * @param t 時刻
    * @return 時刻の文字列
    */
  private[Stage] def intervalString(t:Long):String = {
    val tsec = t / 1000L
    val (d, h, m, s) = (tsec / 24 / 60 / 60, tsec / 60 / 60 % 24, tsec / 60 % 60, tsec % 60)
    if(d > 0) s"${d}日 ${h}時間${m}分" else if(h > 0) s"${h}時間${m}分" else s"${m}分${s}秒"
  }

}