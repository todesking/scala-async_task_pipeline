package com.todesking.async_task_pipeline

import java.util.{concurrent => jc}
import scala.collection.{mutable => mutable}
class BlockingThreadPoolExecutor(corePoolSize:Int, maxPoolSize:Int, keepAliveMillis:Int, queue:jc.BlockingQueue[Runnable])
  extends jc.ThreadPoolExecutor(
    corePoolSize, maxPoolSize,
    keepAliveMillis, jc.TimeUnit.MILLISECONDS,
    queue,
    jc.Executors.defaultThreadFactory,
    new jc.RejectedExecutionHandler() {
      override def rejectedExecution(r:Runnable, executor:jc.ThreadPoolExecutor):Unit = {
        if(executor.isShutdown)
          throw new jc.RejectedExecutionException
        executor.getQueue().put(r)
      }
    }
) {
  def this(corePoolSize: Int, maxPoolSize: Int, keepAliveMillis: Int, queueSize: Int) =
    this(corePoolSize, maxPoolSize, keepAliveMillis, new java.util.concurrent.ArrayBlockingQueue[Runnable](queueSize))

  private[this] var errorHandlers = Seq.empty[Throwable => Unit]

  def onError(h: Throwable => Unit): Unit = synchronized {
    this.errorHandlers = errorHandlers :+ h
  }

  override def afterExecute(r: Runnable, t: Throwable): Unit = {
    if(t != null) {
      synchronized { errorHandlers }.foreach { h =>
        try { h(t) } catch { case e: Throwable => }
      }
    }
  }

  def abort(): Unit = shutdownNow()
}


