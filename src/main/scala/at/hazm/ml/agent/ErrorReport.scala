package at.hazm.ml.agent

import java.io.{PrintWriter, StringWriter}
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._
import scala.collection.mutable

class ErrorReport {
  private[this] val errors = new ConcurrentHashMap[Long,Long]()

  def checkIn(ex:Throwable):Long = {
    val identity = (ex.getClass.getName + "\n" + ex.getStackTrace.map{ s =>
      s"${s.getClassName}.${s.getMethodName}(${s.getFileName}:${s.getLineNumber})"
    }.mkString("\n")).getBytes(StandardCharsets.UTF_8)
    val hash = MessageDigest.getInstance("SHA-256").digest(identity)
    val num = hash.take(8).zipWithIndex.foldLeft(0L){ case (a, (b, i)) => a | (b << (i * 8)) }
    errors.compute(num, (_, value) => value + 1)
  }
}
