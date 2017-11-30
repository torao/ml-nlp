package at.hazm.core.util

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{ArrayBlockingQueue, BlockingQueue}

import at.hazm.core._
import at.hazm.core.util.ResourcePool.Factory

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

/**
  * 固定数のリソースをメモリ上に保持し複数のスレッドで共有するためのクラスです。
  *
  * @param size    リソースのプールサイズ
  * @param factory リソースの生成や破棄を行うインスタンス
  * @tparam T プールするリソースの型
  */
class ResourcePool[T](size:Int, factory:Factory[T]) extends AutoCloseable {

  private[this] val pool:BlockingQueue[T] = new ArrayBlockingQueue(size)
  private[this] val closed = new AtomicBoolean(false)

  size.times(pool.put(factory.acquire()))

  /**
    * プールしているリソースの一つを返します。すべのリソースが使用中の場合は呼び出しがブロックされます。可能であればこのメソッドではなく
    * 仕様スコープを限定し自動で `recycle()` を行う `acquireAndRun()` を使用してください。
    *
    * @return 利用可能なリソース
    */
  def acquire():T = if(!closed.get()) {
    pool.take()
  } else throw new IllegalStateException("resource pool was closed")

  /**
    * 使用済みのリソースを返却します。
    *
    * @param resource 使用済みのリソース
    */
  def recycle(resource:T):Unit = if(factory.error(resource) || closed.get()) {
    factory.dispose(resource)
    if(!closed.get()) {
      pool.put(factory.acquire())
    }
  } else {
    pool.put(resource)
  }

  /**
    * このリソースプールからリソースを `ExecutionContext` を使用して処理を実行します。プール内のすべてのリソース
    * が使用中の場合は処理がブロックされます。
    *
    * @param f        リソースに対して実行する処理
    * @param _context 実行スレッドのコンテキスト
    * @tparam R 処理結果の型
    * @return 処理結果の Future
    */
  def acquireAndRun[R](f:(T) => R)(implicit _context:ExecutionContext):Future[R] = {
    val r = acquire()
    Future {
      try {
        val result = f(r)
        recycle(r)
        result
      } catch {
        case ex:Throwable =>
          factory.dispose(r)
          if(! closed.get()) pool.put(factory.acquire())
          throw ex
      }
    }
  }

  /**
    * このリソースプールが確保しているリソースをすべて開放し以後の `acquire()` 要求を受け付けないようにします。
    */
  override def close():Unit = if(closed.compareAndSet(false, true)) {
    pool.asScala.foreach(r => factory.dispose(r))
  }

}

object ResourcePool {

  /**
    * リソースプールでのリソース管理で使用されます。
    *
    * @tparam T 対象とするリソースの型
    */
  trait Factory[T] {

    /**
      * 新規のリソースを作成します。
      *
      * @return リソース
      */
    def acquire():T

    /**
      * 指定されたリソースを開放します。
      *
      * @param resource 開放するリソース
      */
    def dispose(resource:T):Unit

    /**
      * 指定されたリソース上でエラーが発生していないかを判定します。実行ごとにリソース上でエラーが発生しているかを評価し、エラーが発生して
      * いるリソースは廃棄されます。
      *
      * @param resource 検査するリソース
      * @return エラーが発生している場合 true
      */
    def error(resource:T):Boolean = false
  }

}