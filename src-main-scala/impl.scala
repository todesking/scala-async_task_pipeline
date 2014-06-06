package com.todesking.async_task_pipeline

import java.util.{concurrent => jc}
import scala.collection.{mutable => mutable}

abstract class SourceExecutionContextImpl[A] extends SourceExecutionContext[A] {
  var wiredSink:SinkExecutionContext[A] = ExecutionContext.nullSink()
  private[this] var wired = false
  override def wireTo(sink:SinkExecutionContext[A]):Unit = {
    if(wired)
      throw new RuntimeException("Ahhhhhggggg")
    wiredSink = sink
    wired = true
  }
}

class ConnectPipePipe[A, B, C] (
  val lhs:Pipe[A, B],
  val rhs:Pipe[B, C]
) extends Pipe[A, C] {
  override def run() = new Ctx()

  class Ctx extends PipeExecutionContext[A, C] {
    val leftContext = lhs.run()
    val rightContext = rhs.run()
    leftContext.wireTo(rightContext)

    override def feed(v:A):Unit = leftContext.feed(v)
    override def wireTo(sink:SinkExecutionContext[C]):Unit = rightContext.wireTo(sink)
    override def await():Unit = {
      leftContext.await()
      rightContext.await()
    }
  }
}

class ConnectPipeSink[A, B](
  val lhs:Pipe[A, B],
  val rhs:Sink[B]
) extends Sink[A] {
  override def run() = new Ctx()

  class Ctx extends SinkExecutionContext[A] {
    val leftContext = lhs.run()
    val rightContext = rhs.run()
    leftContext.wireTo(rightContext)

    override def feed(v:A):Unit = leftContext.feed(v)
    override def await():Unit = {
      leftContext.await()
      rightContext.await()
    }
  }
}

class UnorderedPipeImpl[A, B](
  val proc:(A => Option[B]),
  val threadPoolConfig:ThreadPoolConfig
) extends Pipe[A, B] {
  override def run() = new Ctx()

  class Ctx extends SourceExecutionContextImpl[B] with PipeExecutionContext[A, B] {
    val threadPool = threadPoolConfig.createThreadPool()

    override def feed(value:A):Unit = {
      threadPool.execute(new Runnable {
        override def run():Unit = {
          proc(value).map(wiredSink.feed(_))
        }
      })
    }

    override def await():Unit = {
      threadPool.shutdown()
      while(!threadPool.awaitTermination(100, jc.TimeUnit.MILLISECONDS)) ();
    }
  }
}

// A Pipe such that ∀a:A -> current_execution_count(groupOf(A)) <= 1
class UnorderedUniquePipeImpl[A, B, G](
  val groupOf:(A => G),
  proc:(A => Option[B]),
  val retryIntervalMillis:Int,
  threadPoolConfig:ThreadPoolConfig
) extends Pipe[A, B] {
  override def run() = new Ctx()
  class Ctx extends SourceExecutionContextImpl[B] with PipeExecutionContext[A, B] {
    val threadPool = threadPoolConfig.createThreadPool()
    val processing = new mutable.HashSet[G]
    private[this] val lock:AnyRef = this

    override def feed(value:A):Unit = {
      val g = groupOf(value)

      var retry = true
      while(retry) {
        lock.synchronized {
          if(!processing.contains(g)) {
            retry = false
            processing.add(g)
          }
        }
        if(retry) Thread.sleep(retryIntervalMillis)
      }

      // At here, well ensured that current_execution_count(g) == 0
      threadPool.execute(new Runnable {
        override def run():Unit = {
          val result = proc(value)
          lock.synchronized { processing.remove(g) }
          result.map(wiredSink.feed(_))
        }
      })
    }

    override def await():Unit = {
      while(!processing.isEmpty) Thread.sleep(100)

      threadPool.shutdown
      while(!threadPool.awaitTermination(100, jc.TimeUnit.MILLISECONDS)) ();
    }
  }
}


