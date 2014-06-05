package com.todesking.async_task_pipeline

import java.util.{concurrent => jc}
import scala.collection.{mutable => mutable}

object AsyncTaskPipeline {
  val builder = Builder
}

object Builder {
  def pipe[A, B]() = new PipeB[A, B]

  def sinkToGrowable[A](dest:scala.collection.generic.Growable[A]) = new SinkToGrowable[A](dest)

  class PipeB[A, B] {
    def unordered(proc:A => Option[B]):Pipe[A, B] = {
      new UnorderdPipeImpl[A, B](
        proc,
        ThreadPoolConfig.default()
      )
    }
    def unordered() = new Unordered

    class Unordered {
      def unique[G](groupOf:A => G)(proc:A => Option[B]) = new UnorderedUniquePipeImpl(
        groupOf, proc,
        10,
        ThreadPoolConfig.default()
      )
    }
  }
}

trait ExecutionContext {
  // Close pipe and await all process finished
  def await():Unit
}

object ExecutionContext {
  def nullSink[A]():SinkExecutionContext[A] =
    new SinkExecutionContext[A]() {
      def feed(value:A):Unit = ()
      def await():Unit = ()
    }
}

trait Closure {
  def run():ExecutionContext
}

trait SinkExecutionContext[A] extends ExecutionContext {
  def feed(value:A):Unit
}
trait Sink[A] extends Closure {
  override def run():SinkExecutionContext[A]
}

class SinkToGrowable[A](val dest:scala.collection.generic.Growable[A]) extends Sink[A] {
  override def run() = new Ctx

  class Ctx extends SinkExecutionContext[A] {
    override def await() = ()
    override def feed(value:A) = synchronized { dest += value }
  }
}

trait SourceExecutionContext[A] extends ExecutionContext {
  def wireTo(sink:SinkExecutionContext[A]):Unit
}
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

trait Source[A] extends Closure {
  def >>(sink:Sink[A]):Closure
  def >>[B](pipe:Pipe[A, B]):Source[B]
  override def run():SourceExecutionContext[A]
}

trait PipeExecutionContext[A, B] extends ExecutionContext with SinkExecutionContext[A] with SourceExecutionContext[B]
trait Pipe[A, B] extends Closure with Sink[A] with Source[B] {
  def >>(sink:Sink[B]):Sink[A] = new ConnectPipeSink[A, B](this, sink)
  def >>[C](pipe:Pipe[B, C]):Pipe[A, C] = new ConnectPipePipe[A, B, C](this, pipe)
  override def run():PipeExecutionContext[A, B]
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

case class ThreadPoolConfig(
  queueSize:Int,
  corePoolSize:Int,
  maxPoolSize:Int,
  keepAliveMillis:Int
) {
  def createThreadPool() =
    new BlockingThreadPoolExecutor(
      corePoolSize, maxPoolSize,
      keepAliveMillis,
      new jc.ArrayBlockingQueue[Runnable](queueSize)
    )
}

object ThreadPoolConfig {
  def default():ThreadPoolConfig = ThreadPoolConfig(
    queueSize = 10,
    corePoolSize = 1,
    maxPoolSize = 10,
    keepAliveMillis = 100
  )
}

class UnorderdPipeImpl[A, B](
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
) extends UnorderdPipeImpl[A, B](proc, threadPoolConfig) {
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


object Main {
  import java.net.URL
  def main(args:Array[String]):Unit = {
    val src:TraversableOnce[URL] = Seq()

    val builder = AsyncTaskPipeline.builder

    type Domain = String
    def domainOf(url:URL):Domain = ""

    def crawlable(url:URL):Boolean = true

    type PageContent = String
    def fetch(url:URL):PageContent = ""

    val filterByRule = builder.pipe[URL, URL].unordered {url => Some(url)}
    val filterByRobtsTxt = builder.pipe[URL, URL].unordered.unique(domainOf(_)) {url => if(crawlable(url)) Some(url) else None }
    val crawl = builder.pipe[URL, PageContent].unordered.unique(domainOf(_)) {url => Some(fetch(url)) }

    val activePipe = (filterByRule >> filterByRobtsTxt >> crawl).run

    src.foreach(activePipe.feed(_))

    activePipe.await()
  }
}


