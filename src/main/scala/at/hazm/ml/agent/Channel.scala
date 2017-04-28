package at.hazm.ml.agent

import java.io.File
import java.util.{Timer, TimerTask}

import at.hazm.ml.io.Database._
import at.hazm.ml.io.{Database, readBinary}
import org.slf4j.LoggerFactory
import twitter4j.{Status, StatusUpdate, TwitterFactory}
import twitter4j.conf.PropertyConfiguration

import scala.collection.JavaConverters._
import scala.io.Source

trait Channel[Q, A]{
  def start():Unit

  def stop():Unit
  def ask(question:Q):Option[A]
}

object Channel {
  private[Channel] val logger = LoggerFactory.getLogger(classOf[Channel[_, _]])

  /**
    * ポーリング型のチャネルが共有するタイマーです。
    */
  val pollingTimer = new Timer("ChannelPollingTimer", true)

  type CALLBACK[Q,A] = (Channel[Q,A],Q)=>Option[A]

  abstract class PollingChannel[Q, A] extends Channel[Q, A] {
    private[this] var task:Option[TimerTask] = None
    private[this] val report = new ErrorReport()

    protected def startPolling(interval:Long):Unit = {
      stopPolling()
      task = Some(new TimerTask() {
        def run():Unit = try {
          poll()
        } catch {
          case ex:Throwable =>
            val count = report.checkIn(ex)
            if(count == 1) {
              logger.error("unexpected exception in polling channel", ex)
            } else {
              logger.error(f"unexpected exception in polling channel ($count%,d)")
            }
        }
      })
      task.foreach { t => pollingTimer.scheduleAtFixedRate(t, 0, interval) }
    }

    protected def stopPolling():Unit = task.foreach { t =>
      task = None
      t.cancel()
    }

    protected def poll():Unit
  }

  abstract class Twitter(conf:File, db:Database, responseLimitMin:Int = 5) extends PollingChannel[twitter4j.Status, String] {
    db.trx { con =>
      con.createTable(
        """twitter_tweet(
          |  id integer not null primary key autoincrement,
          |  user_id bigint not null,
          |  screen_name text not null,
          |  asked_at timestamp not null,
          |  asked_text text not null,
          |  replied_at timestamp not null,
          |  replied_text text not null
          |)""".stripMargin)
    }
    private[this] var twitter:Option[twitter4j.Twitter] = None

    override def start():Unit = {
      val c = readBinary(conf) { in => new PropertyConfiguration(in) }
      this.twitter = Some(new TwitterFactory(c).getInstance())
      startPolling(15 * 1000L)
    }

    override def stop():Unit = this.twitter.foreach { _ =>
      stopPolling()
      this.twitter = None
    }

    protected def poll():Unit = twitter.foreach { t =>
      val user = t.verifyCredentials()
      db.trx { con =>
        val tm = con.headOption("select max(asked_at) from twitter_tweet")(rs=>Option(rs.getTimestamp(1))).flatten.map(_.getTime).getOrElse(0L)

        t.getMentionsTimeline.asScala.filter { status =>
          status.getCreatedAt.getTime > math.max(tm, System.currentTimeMillis() - responseLimitMin * 60 * 1000L) &&
            status.getUser.getId != user.getId
        }.sortBy(_.getCreatedAt.getTime).foreach { status =>
          ask(status).foreach { response =>
            val reply = new StatusUpdate(response)
            reply.setInReplyToStatusId(status.getId)
            val result = t.updateStatus(reply)
            con.exec("insert into twitter_tweet(user_id,screen_name,asked_at,asked_text,replied_at,replied_text) values(?,?,?,?,?,?)",
              status.getUser.getId, status.getUser.getScreenName,
              new java.sql.Timestamp(status.getCreatedAt.getTime), status.getText,
              new java.sql.Timestamp(result.getCreatedAt.getTime), result.getText)
          }
        }
      }
    }
  }

  abstract class ConsoleChannel extends Channel[String,String]{
    private[this] val thread = new Thread(()=> {
      System.out.print("> ")
      System.out.flush()
      Source.fromInputStream(System.in).getLines.foreach{ line =>
        ask(line) match {
          case Some(reply) => System.out.println(s">>> $reply")
          case None => System.out.println("??? not_respond")
        }
        System.out.print("> ")
        System.out.flush()
      }
    })
    thread.setDaemon(true)
    thread.start()
    override def start():Unit = ()

    override def stop():Unit = ()
  }

}