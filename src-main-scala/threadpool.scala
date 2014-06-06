package com.todesking.async_task_pipeline

import java.util.{concurrent => jc}
import scala.collection.{mutable => mutable}

case class ThreadPoolConfig(
  queueSize:Int = 10,
  corePoolSize:Int = 1,
  maxPoolSize:Int = 10,
  keepAliveMillis:Int = 100
) {
  def createThreadPool() =
    new BlockingThreadPoolExecutor(
      corePoolSize, maxPoolSize,
      keepAliveMillis,
      new jc.ArrayBlockingQueue[Runnable](queueSize)
    )
}

object ThreadPoolConfig {
  def default():ThreadPoolConfig = ThreadPoolConfig()
}

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
}


