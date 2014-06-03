package com.todesking.async_task_pipeline

import java.util.{concurrent => jc}

trait ExecutionContext
object ExecutionContext {
  def nullSink[A]():SinkExecutionContext[A] =
    new SinkExecutionContext[A]() {
      def feed(value:A):Unit = ()
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

trait SourceExecutionContext[A] extends ExecutionContext {
  def wireTo(sink:SinkExecutionContext[A]):Unit
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

    def feed(v:A):Unit = leftContext.feed(v)
  }
}

class UnorderdPipeImpl[A, B](
  val proc:(A => B),
  val queueSize:Int,
  val corePoolSize:Int,
  val maxPoolSize:Int,
  val keepAliveMillis:Int
) extends Pipe[A, B] {
  def run() = new Ctx()

  class Ctx extends PipeExecutionContext[A, B] {
    val threadPool = new BlockingThreadPoolExecutor(
      corePoolSize, maxPoolSize,
      keepAliveMillis,
      new jc.ArrayBlockingQueue[Runnable](queueSize)
    )

    var wiredSink:SinkExecutionContext[B] = ExecutionContext.nullSink()

    override def feed(value:A):Unit = {
      threadPool.execute(new Runnable {
        override def run():Unit = {
          wiredSink.feed(proc(value))
        }
      })
    }

    override def wireTo(sink:SinkExecutionContext[B]):Unit = { this.wiredSink = sink }
  }
}

class UnorderedGroupedPipeImpl[A, B, G](
  val queueSizePerGroup:Int,
  val maxProcessItems:Int,
  val queueSize:Int,
  val corePoolSize:Int,
  val maxPoolSize:Int,
  val keepAliveMillis:Int
) extends Pipe[A, B] {
  def run() = new Ctx()

  class Ctx extends PipeExecutionContext[A, B] {
    val threadPool = new BlockingThreadPoolExecutor(
      corePoolSize, maxPoolSize,
      keepAliveMillis,
      new jc.ArrayBlockingQueue[Runnable](maxPoolSize)
    )
    val queue:jc.ArrayBlockingQueue[jc.ArrayBlockingQueue[s]]

    var wiredSink:SinkExecutionContext[B] = ExecutionContext.nullSink()

    override def feed(value:A):Unit = {
    }

    override def wireTo(sink:SinkExecutionContext[B]):Unit = { this.wiredSink = sink }
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
        executor.getQueue().put(r)
      }
    }
) {
}


object Main {
  def main(args:Array[String]):Unit = {
    /*
    val src:TraversableOnce[URL]

    val pipe = Pipeline.builder

    val filterByRule = Pipe.unordered[URL, URL] {url => if(canCrawl(url)) emit(url) }
    val filterByRobotsTxt = Pipe.unordered[URL, URL].grouped[Domain] {(domain, urls, out) =>
      def findRule(domain:Domain):RobotsTxt = {
        RobtsTxtCache.findOrRead(domain)
      }
      val robots = findRule(domain)
      urls.foreach(url => if(robots.canCrawl(url)) out.emit(url))
    }
    val throttle = Pipe.unordered.grouped[URL, URL, Domain] {(domain, urls) =>
      urls.foreach {url =>
        DomainStatus.waitUntilCrawlable(domain)
        emit(url)
      }
    }
    val crawl = Pipe.unordered.grouped[URL, PageContent, Domain] {(domain, urls, out) =>
      def httpGet(url) = { DomainStatus.crawling(domain) { rawGet(url) } }
      urls.foreach {url=> out.emit(new PageContent(httpGet(url))) }
    }
    val persistContent = Pipe.tap[PageContent](repository[PageContent].update(_))

    val analyze = Pipeline.unordered[PageContent, PageKeyword](analyze(_))

    val persistKeyword = Pipe.tap[PageKeyword](save(_))

    val pipeline:UnorderdPipeline[URL, PageContent] = filterByRule >> filterByRobotsTxt >> throttle >> crawl >> persistContent|(analyze >> persistKeyword)

    pipeline.emit(src)
    pipeline.await()
    */
  }
}


