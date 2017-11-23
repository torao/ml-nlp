package at.hazm.core.util

import java.lang.management.ManagementFactory
import java.text.DateFormat
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import java.util.{Timer, TimerTask}
import javax.management.ObjectName

import at.hazm.core.util.Diagnostics.Performance
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.concurrent.Future

private[util] class Diagnostics {
  val performance:mutable.HashMap[String, Performance] = new mutable.HashMap()
  val report = new Diagnostics.Report()
}

object Diagnostics {
  private[Diagnostics] val logger = LoggerFactory.getLogger(classOf[Diagnostics])

  private[this] val diagnostics = new ThreadLocal[Diagnostics]() {
    override def initialValue():Diagnostics = new Diagnostics()
  }

  private[this] val timer = new Timer("Diagnostics", true)

  def measure[T](id:String)(f: => T):T = {
    val t0 = System.currentTimeMillis()
    try {
      f
    } finally {
      val t = System.currentTimeMillis() - t0
      val p = diagnostics.get().performance.getOrElseUpdate(id, new Performance())
      p.callCount += 1
      p.totalTime += t
      p.max = if(p.max < 0) t else math.max(p.max, t)
      p.min = if(p.max < 0) t else math.min(p.max, t)
    }
  }

  def measureAsync[T](id:String)(f: => Future[T]):Future[T] = {
    import scala.concurrent.ExecutionContext.Implicits.global
    val t0 = System.currentTimeMillis()
    f.andThen { case _ =>
      val t = System.currentTimeMillis() - t0
      val p = diagnostics.get().performance.getOrElseUpdate(id, new Performance())
      p.callCount += 1
      p.totalTime += t
      p.max = if(p.max < 0) t else math.max(p.max, t)
      p.min = if(p.max < 0) t else math.min(p.max, t)
    }
  }

  /**
    * 処理の進捗状況を表示するためのクラス。
    * {{{
    *   val prog = new Progress(file.getName, 0, file.length)
    *   while(...){
    *     prog.report(current, data)
    *   }
    * }}}
    * ファイル入力を進捗表示するには [[at.hazm.core.io.ProgressInputStream]] を使用してください。
    *
    * @param prefix   ログのプレフィクス
    * @param init     開始時の初期値
    * @param max      与えられうる最大値
    * @param interval ログ出力間隔 (ミリ秒)
    */
  class Progress(prefix:String, init:Long, max:Long, interval:Long = 60 * 1000L) {
    private[this] val diag = diagnostics.get()
    private[this] val t0 = System.currentTimeMillis()
    private[this] val _current = new AtomicLong(0L)
    private[this] val task = new TimerTask {
      override def run():Unit = print()
    }
    private[this] val _stopped = new AtomicBoolean(false)

    private[this] val history = mutable.Buffer[(Long, Long)]()
    history.append((t0, init))

    timer.scheduleAtFixedRate(task, interval, interval)

    def apply(f:(Progress) => Unit):Unit = try {
      f(this)
    } catch {
      case ex:Break => ()
    } finally {
      stop()
    }

    /**
      * 現在の進捗値を参照します。
      */
    def current:Long = _current.get()

    /**
      * 指定された状況メッセージをレポートします。このメソッドは進捗値をインクリメントします。
      *
      * @param message 状況メッセージ
      */
    def report(message:String):Unit = _report(_current.incrementAndGet(), message)

    /**
      * 指定された進捗値を現在の進捗として状況メッセージをレポートします。
      *
      * @param current 進捗
      * @param message 状況メッセージ
      */
    def report(current:Long, message:String):Unit = {
      this._current.set(current)
      _report(current, message)
    }

    def report():Unit = report("")

    def report(current:Long):Unit = report(current, "")

    private[this] def _report(current:Long, message:String = ""):Unit = {
      this._current.set(current)
      diag.report.current = current
      diag.report.message = message
      diag.report.tm = System.currentTimeMillis()
      if(current == max) {
        stop()
        print()
      } else if(current == init + 1) {
        print()
      }
      if(_stopped.get()) {
        throw new Break()
      }
    }

    def stopped:Boolean = _stopped.get

    private[this] def stop():Unit = {
      _stopped.set(true)
      val server = ManagementFactory.getPlatformMBeanServer
      server.unregisterMBean(mxBeanName)
      task.cancel()
    }

    private[this] def print():Unit = {
      val tm = diag.report.tm
      val current = diag.report.current
      val message = diag.report.message
      val time = if(current == max) {
        s"所要時間 ${intervalString(tm - t0)}"
      } else {
        // 履歴に進捗を追加
        history.append((tm, current + init))
        while(history.size > (60 * 60 * 1000L) / interval) {
          history.remove(0)
        }

        val t = estimateEndTime()
        if(t < 0) {
          s"終了予想時間計測中"
        } else {
          s"残り ${intervalString(t - tm)}"
        }
      }
      val msg = if(message.isEmpty) "" else s": $message"
      logger.info(f"$prefix: $current%,d / $max%,d (${current * 100.0 / max}%.1f%%) $time$msg")
      if(diag.performance.nonEmpty) {
        logger.info(diag.performance.map { case (id, p) => f"$id:avr=${p.totalTime / p.callCount}%,dms,min=${p.min}%,dms,max=${p.max}%,dms/${p.callCount}%,dcall" }.mkString(" "))
      }
    }

    private[this] def estimateEndTime():Long = {
      // 最小二乗法で傾き a と切片 b を計算
      val n = history.size
      val Σxy = history.map(t => t._1.toDouble * t._2).sum
      val Σx = history.map(t => t._1.toDouble).sum
      val Σy = history.map(t => t._2.toDouble).sum
      val Σx2 = history.map(t => t._1.toDouble * t._1).sum
      val a = (n * Σxy - Σx * Σy) / (n * Σx2 - Σx * Σx)
      val b = (Σx2 * Σy - Σxy * Σx) / (n * Σx2 - Σx * Σx)
      if(a.isInfinity || b.isInfinite) -1 else ((max - b) / a).toLong
    }

    private[this] class Break extends Exception

    private[this] class MXBean extends ProgressMXBean {
      override def getStart:String = DateFormat.getDateTimeInstance(DateFormat.MEDIUM, DateFormat.SHORT).format(t0)

      override def getEnd:String = DateFormat.getDateTimeInstance(DateFormat.MEDIUM, DateFormat.SHORT).format(estimateEndTime())

      override def getCurrent:Long = current

      override def getFinished:Long = init + current

      override def getTotal:Long = max

      override def shutdown():Unit = if(_stopped.compareAndSet(false, true)) {
        logger.info("shutting down")
      }
    }

    private[this] val mxBeanName = new ObjectName(s"at.hazm.diag:name=$prefix")
    locally {
      val server = ManagementFactory.getPlatformMBeanServer
      val mbean = new MXBean()
      server.registerMBean(mbean, mxBeanName)
    }

  }

  private[Diagnostics] class Report {
    var current = 0L
    var message = ""
    var tm:Long = System.currentTimeMillis()
  }

  private[Diagnostics] class Performance {
    var callCount = 0L
    var totalTime = 0L
    var max:Long = -1L
    var min:Long = -1L
  }

  private[this] def intervalString(t:Long):String = {
    val tsec = t / 1000L
    val (d, h, m, s) = (tsec / 24 / 60 / 60, tsec / 60 / 60 % 24, tsec / 60 % 60, tsec % 60)
    if(d > 0) s"${d}日 ${h}時間${m}分" else if(h > 0) s"${h}時間${m}分" else s"${m}分${s}秒"
  }

}
