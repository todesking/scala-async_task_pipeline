package com.todesking.async_task_pipeline

import scala.concurrent.ExecutionContext

import scala.language.existentials

sealed trait Dataflow {
  def named(name: String): Dataflow
}

sealed trait Sink[-A] extends Dataflow {
  override def named(name: String): Sink[A] =
    Dataflow.NamedSink(name, this)
}

sealed trait Pipe[-A, +B] extends Dataflow with Sink[A] {
  def >>~(sink: Sink[B]): Sink[A] =
    Dataflow.PipeToSink(this, sink)

  def >>>[C](pipe: Pipe[B, C]): Pipe[A, C] =
    Dataflow.PipeToPipe(this, pipe)

  def <=>[AA <: A, BB >: B](pipe: Pipe[AA, BB]): Pipe[AA, BB] =
    Dataflow.ForkJoin[AA, BB](this, pipe)

  override def named(name: String): Pipe[A, B] =
    Dataflow.NamedPipe(name, this)
}

object Dataflow {
  def buildPipe[A, B](p: Parallelism)(f: A => Seq[B]): Pipe[A, B] =
    Dataflow.PipeImpl(p, f)

  def buildSink[A](p: Parallelism)(f: A => Unit): Sink[A] =
    Dataflow.SinkImpl(p, f)

  def buildSinkToGrowable[A](g: scala.collection.generic.Growable[A]): Sink[A] =
    buildSink[A](Parallelism.Constant(1)) { value: A =>
      g += value
    }

  def serialize[A, B, C](bufferSize: Int)(key: A => C)(pipe: Pipe[A, B]): Serialize[A, B, C] =
    Serialize(bufferSize, pipe, key)

  def runSink[A](sink: Sink[A]): SinkExecution[A] = sink match {
    case SinkImpl(p, f) =>
      p.runSink(f)
    case PipeToSink(pipe, sink) =>
      runPipe(pipe) >>~ runSink(sink)
    case NamedSink(name, sink) =>
      DataflowExecution.NamedSink(name, runSink(sink))
    case p: Pipe[A, _] =>
      runPipe(p)
  }

  def runPipe[A, B](pipe: Pipe[A, B]): PipeExecution[A, B] = pipe match {
    case PipeImpl(p, f) =>
      p.runPipe(f)
    case PipeToPipe(pipe1, pipe2) =>
      runPipe(pipe1) >>> runPipe(pipe2)
    case ForkJoin(left, right) =>
      runPipe(left) <=> runPipe(right)
    case NamedPipe(name, pipe) =>
      DataflowExecution.NamedPipe(name, runPipe(pipe))
    case Serialize(queue, pipe, key) =>
      DataflowExecution.Serialize(queue, runPipe(pipe), key, {(_: A, _: Int) => true}).collect { case Right(a) => a }
    case p @ SerializeWithRejected(queue, pipe, key, retry) =>
      DataflowExecution.Serialize(queue, runPipe[A, p.OutType](pipe), key, retry)
  }

  case class NamedSink[A](name: String, sink: Sink[A]) extends Sink[A]

  case class SinkImpl[A](p: Parallelism, f: A => Unit) extends Sink[A]

  case class NamedPipe[A, B](name: String, pipe: Pipe[A, B]) extends Pipe[A, B]

  case class PipeImpl[A, B](p: Parallelism, f: A => Seq[B]) extends Pipe[A, B]

  case class PipeToSink[A, B](pipe: Pipe[A, B], sink: Sink[B]) extends Sink[A]

  case class PipeToPipe[A, B, C](pipeAB: Pipe[A, B], pipeBC: Pipe[B, C]) extends Pipe[A, C]

  case class ForkJoin[A, B](left: Pipe[A, B], right: Pipe[A, B]) extends Pipe[A, B]

  case class Serialize[A, B, C](queueSize: Int, pipe: Pipe[A, B], key: A => C) extends Pipe[A, B] {
    def retry(r: (A, Int) => Boolean): SerializeWithRejected[A, B, C] =
      SerializeWithRejected(queueSize, pipe, key, r)
  }

  case class SerializeWithRejected[A, B, C](queueSize: Int, pipe: Pipe[A, B], key: A => C, shouldRetry: (A, Int) => Boolean) extends Pipe[A, Either[A, B]] {
    type OutType = B
  }
}

trait Parallelism {
  def runSink[A](f: A => Unit): SinkExecution[A]
  def runPipe[A, B](f: A => Seq[B]): PipeExecution[A, B]
}

object Parallelism {
  case class Constant(threadNum: Int) extends Parallelism {
    override def runSink[A](f: A => Unit): SinkExecution[A] = runPipe { value => f(value); Seq.empty }

    override def runPipe[A, B](f: A => Seq[B]): PipeExecution[A, B] = DataflowExecution.ConstantPipe(threadNum, f)
  }

  case class Buffer(size: Int) extends Parallelism {
    override def runSink[A](f: A => Unit): SinkExecution[A] = runPipe[A, Unit] { v => f(v); Seq.empty }
    override def runPipe[A, B](f: A => Seq[B]): PipeExecution[A, B] = DataflowExecution.BufferPipe[A, B](size, f)
  }
}

trait DataflowExecution {
  def statusString(): String
  def getSink(name: String): Option[SinkExecution[_]]
  def abort(): Unit
  protected def awaitInternal(): Unit

  final def await(): Unit = {
    awaitInternal()
    synchronized {
      if(exception != null)
        throw new RuntimeException(exception)
    }
  }

  protected def setException(e: Throwable): Unit = synchronized {
    println(e)
    this.exception = e
  }
  private[this] var exception: Throwable = null
}

trait SinkExecution[-A] extends DataflowExecution {
  def feed(value: A): Unit
}

trait SourceExecution[+A] extends DataflowExecution {
}

trait PipeExecution[-A, +B] extends DataflowExecution with SinkExecution[A] with SourceExecution[B] {
  def executionContext: ExecutionContext

  override def feed(value: A): Unit =
    feedPipe1(value) { _ => }

  // callback is executed when all value is calculated
  def feedPipe(value: A)(callback: Seq[B] => Unit): Unit

  // callback is executed when some value is calculated
  def feedPipe1(value: A)(callback: B => Unit): Unit =
    feedPipe(value)(_.foreach(callback))

  def >>>[C](pipe: PipeExecution[B, C]): PipeExecution[A, C] =
    DataflowExecution.PipeToPipe(this, pipe)

  def >>~(sink: SinkExecution[B]): SinkExecution[A] =
    DataflowExecution.PipeToSink(this, sink)

  def <=>[AA <: A, BB >: B](right: PipeExecution[AA, BB]): PipeExecution[AA, BB] =
    DataflowExecution.ForkJoin(this, right)

  def collect[C](f: PartialFunction[B, C]): PipeExecution[A, C] =
    DataflowExecution.Collect(this, f.lift)
}

object DataflowExecution {
  case class Collect[A, B, C](pipe: PipeExecution[A, B], f: B => Option[C]) extends PipeExecution[A, C] {
    override def executionContext = pipe.executionContext
    override def awaitInternal() = pipe.await()
    override def statusString = s"${pipe.statusString}"
    override def feed(value: A) = pipe.feedPipe1(value) { b => f(b) }
    override def feedPipe(value: A)(cb: Seq[C] => Unit) = pipe.feedPipe(value) { bs => cb(bs.map(f).flatMap(_.toSeq)) }
    override def feedPipe1(value: A)(cb: C => Unit) = pipe.feedPipe1(value) { b => f(b).toSeq.foreach(cb) }
    override def getSink(name: String) = pipe.getSink(name)
    override def abort() = pipe.abort()
  }
  case class PipeToPipe[A, B, C](left: PipeExecution[A, B], right: PipeExecution[B, C]) extends PipeExecution[A, C] {
    override def executionContext = right.executionContext

    override def abort(): Unit = {
      left.abort()
      right.abort()
    }

    override def awaitInternal(): Unit = {
      left.await()
      right.await()
    }

    override def feedPipe(value: A)(callback: Seq[C] => Unit): Unit =
      left.feedPipe(value) { vs1 =>
        val results = scala.collection.mutable.ArrayBuffer.empty[C]
        val countdown = new java.util.concurrent.atomic.AtomicInteger(vs1.size)
        vs1.foreach { v2 =>
          right.feedPipe(v2) { vs3 =>
            results.synchronized { results ++= vs3 }
            if(countdown.decrementAndGet() == 0) {
              callback(results)
            }
          }
        }
      }

    override def feedPipe1(value: A)(cb: C => Unit) =
      left.feedPipe1(value) { v1 =>
        right.feedPipe1(v1)(cb)
      }

    override def statusString = s"${left.statusString} >>> ${right.statusString}"

    override def getSink(name: String) = left.getSink(name) orElse right.getSink(name)
  }

  case class NamedPipe[A, B](name: String, pipe: PipeExecution[A, B]) extends PipeExecution[A, B] {
    override def executionContext = pipe.executionContext
    override def awaitInternal() = pipe.await()
    override def statusString = s"${name}:[${pipe.statusString}]"
    override def feed(value: A) = pipe.feed(value)
    override def feedPipe(value: A)(cb: Seq[B] => Unit) = pipe.feedPipe(value)(cb)
    override def feedPipe1(value: A)(cb: B => Unit) = pipe.feedPipe1(value)(cb)
    override def getSink(name: String) = if(name == this) Some(this) else pipe.getSink(name)
    override def abort() = pipe.abort()
  }

  case class NamedSink[A](name: String, sink: SinkExecution[A]) extends SinkExecution[A] {
    override def awaitInternal() = sink.await()
    override def statusString = s"${name}:[${sink.statusString}]"
    override def feed(value: A) = sink.feed(value)
    override def getSink(name: String) = if(name == this) Some(this) else sink.getSink(name)
    override def abort() = sink.abort()
  }

  case class PipeToSink[A, B](left: PipeExecution[A, B], right: SinkExecution[B]) extends SinkExecution[A] {
    override def abort() = {
      left.abort()
      right.abort()
    }

    override def awaitInternal(): Unit = {
      left.await()
      right.await()
    }

    override def feed(value: A): Unit =
      left.feedPipe1(value) { v => right.feed(v) }

    override def statusString = s"${left.statusString} >>~ ${right.statusString}"

    override def getSink(name: String) = left.getSink(name) orElse right.getSink(name)
  }

  case class ForkJoin[A, B](left: PipeExecution[A, B], right: PipeExecution[A, B]) extends PipeExecution[A, B] {
    private[this] val outBuffer = BufferPipe(1, {v: B => Seq(v) })

    override def abort() = {
      left.abort()
      right.abort()
      outBuffer.abort()
    }

    override def executionContext = outBuffer.executionContext

    override def awaitInternal(): Unit = {
      left.await()
      right.await()
      outBuffer.await()
    }

    override def feedPipe(value: A)(callback: Seq[B] => Unit): Unit = {
      val countdown = new java.util.concurrent.atomic.AtomicInteger(2)
      val results = scala.collection.mutable.ArrayBuffer.empty[B]
      def feedToOut(bs: Seq[B]): Unit = {
        results.synchronized { results ++= bs }
        if(countdown.decrementAndGet() == 0) {
          callback(results)
          results.foreach(outBuffer.feed(_))
        }
      }
      left.feedPipe(value)(feedToOut)
      right.feedPipe(value)(feedToOut)
    }

    override def feedPipe1(value: A)(callback: B => Unit): Unit = {
      def feedToOut(b: B): Unit =
        outBuffer.feedPipe1(b)(callback)

      left.feedPipe1(value)(feedToOut)
      right.feedPipe1(value)(feedToOut)
    }

    override def statusString = s"(${left.statusString()} <=> ${right.statusString()}) >>> ${outBuffer.statusString()}"

    override def getSink(name: String) = left.getSink(name) orElse right.getSink(name)
  }

  case class ConstantPipe[A, B](threadNum: Int, f: A => Seq[B]) extends PipeExecution[A, B] {
    private[this] val pool = new BlockingThreadPoolExecutor(threadNum, threadNum, 1000, 1)

    pool.onError(setException)

    override def abort() = pool.abort()

    override val executionContext = ExecutionContext.fromExecutor(pool)

    override def awaitInternal(): Unit = {
      pool.shutdown()
      while(!pool.awaitTermination(100, java.util.concurrent.TimeUnit.MILLISECONDS)) ();
    }

    override def feedPipe(value: A)(callback: Seq[B] => Unit): Unit = {
      executionContext.execute(new Runnable { override def run(): Unit = { callback(f(value)) } })
    }

    override def statusString = s"{threads=${pool.getActiveCount}/${pool.getPoolSize}}"

    override def getSink(name: String) = None
  }

  case class BufferPipe[A, B](size: Int, f: A => Seq[B]) extends PipeExecution[A, B] {
    private[this] val pool = new BlockingThreadPoolExecutor(1, 1, 1000, size)

    pool.onError(setException)

    override def abort() = pool.abort()

    private[this] def feedInternal(value: A, callback: Seq[B] => Unit) =
      pool.execute(new Runnable {
        override def run() {
          callback(f(value))
        }
      })

    override def executionContext = ExecutionContext.fromExecutor(pool)

    override def awaitInternal(): Unit = {
      pool.shutdown()
      while(!pool.awaitTermination(100, java.util.concurrent.TimeUnit.MILLISECONDS)) ();
    }

    override def feedPipe(value: A)(callback: Seq[B] => Unit): Unit =
      feedInternal(value, callback)

    override def statusString = s"buffer(${size - pool.getQueue.remainingCapacity}/${size})"

    override def getSink(name: String) = None
  }

  case class Serialize[A, B, C](bufferSize: Int, pipe: PipeExecution[A, B], keyOf: A => C, retry: (A, Int) => Boolean) extends PipeExecution[A, Either[A, B]] {
    type OutType = B
    private[this] val buffer = BufferPipe(bufferSize, {v: (A, Int) => Seq(v) })
    private[this] var running = scala.collection.mutable.Set.empty[C]
    private[this] val queueCount = new java.util.concurrent.atomic.AtomicInteger(0)
    private[this] val requeueQueue = new java.util.concurrent.LinkedBlockingQueue[Runnable]()
    private[this] val requeueThreads = new BlockingThreadPoolExecutor(1, 1, 1000, requeueQueue)

    requeueThreads.onError(setException)

    override def abort() = {
      requeueThreads.abort()
      buffer.abort()
      pipe.abort()
    }

    override def executionContext = pipe.executionContext

    override def getSink(name: String) = pipe.getSink(name)

    override def awaitInternal(): Unit = {
      while(queueCount.get > 0) Thread.sleep(0)

      requeueThreads.shutdown()
      while(!requeueThreads.awaitTermination(100, java.util.concurrent.TimeUnit.MILLISECONDS)) ();
      buffer.await()
      pipe.await()
    }

    override def feedPipe(value: A)(callback: Seq[Either[A, B]] => Unit): Unit = {
      queueCount.incrementAndGet()
      feedPipeInternal((value -> 0), keyOf(value), callback)
    }

    def feedPipeInternal(value: (A, Int), key: C, callback: Seq[Either[A, B]] => Unit): Unit = {
      buffer.feedPipe(value) { _ =>
        val go = synchronized {
          if(!running.contains(key)) {
            running += key
            true
          } else {
            false
          }
        }

        if(go) {
          pipe.feedPipe(value._1) { vs =>
            synchronized { running -= key }
            callback(vs.map(Right(_)))
          }
          queueCount.decrementAndGet()
        } else if(retry(value._1, value._2)) {
          requeueThreads.execute(new Runnable {
            override def run(): Unit = {
              feedPipeInternal(value._1 -> (value._2 + 1), key, callback)
            }
          })
        } else {
          pipe.executionContext.execute(new Runnable { override def run(): Unit = { callback(Seq(Left(value._1))) } })
          queueCount.decrementAndGet()
        }
      }
    }

    override def statusString = s"serialized(buffer={${buffer.statusString()}+{${requeueQueue.size}}}, executor={${pipe.statusString()}})"
  }
}
