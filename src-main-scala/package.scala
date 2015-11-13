package com.todesking.async_task_pipeline

import scala.concurrent.Future
import scala.concurrent.ExecutionContext

import scala.language.existentials

import scalaz.std.scalaFuture._
import scalaz.std.vector._
import scalaz.std.indexedSeq._
import scalaz.syntax.monad._

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

  def serialize[A, B, C](bufferSize: Int)(pipe: Pipe[A, B])(key: A => C): Pipe[A, B] =
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
      DataflowExecution.Serialize(queue, runPipe(pipe), key)
  }

  case class NamedSink[A](name: String, sink: Sink[A]) extends Sink[A]

  case class SinkImpl[A](p: Parallelism, f: A => Unit) extends Sink[A]

  case class NamedPipe[A, B](name: String, pipe: Pipe[A, B]) extends Pipe[A, B]

  case class PipeImpl[A, B](p: Parallelism, f: A => Seq[B]) extends Pipe[A, B]

  case class PipeToSink[A, B](pipe: Pipe[A, B], sink: Sink[B]) extends Sink[A]

  case class PipeToPipe[A, B, C](pipeAB: Pipe[A, B], pipeBC: Pipe[B, C]) extends Pipe[A, C]

  case class ForkJoin[A, B](left: Pipe[A, B], right: Pipe[A, B]) extends Pipe[A, B]

  case class Serialize[A, B, C](queueSize: Int, pipe: Pipe[A, B], key: A => C) extends Pipe[A, B]
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
  def await(): Unit
}

trait SinkExecution[-A] extends DataflowExecution {
  def feed(value: A): Unit
}

trait SourceExecution[+A] extends DataflowExecution {
}

trait PipeExecution[-A, +B] extends DataflowExecution with SinkExecution[A] with SourceExecution[B] {
  def executionContext: ExecutionContext

  override def feed(value: A): Unit =
    feedPipe(value) { _ => }

  def feedPipe(value: A)(callback: B => Unit): Unit

  def >>>[C](pipe: PipeExecution[B, C]): PipeExecution[A, C] =
    DataflowExecution.PipeToPipe(this, pipe)

  def >>~(sink: SinkExecution[B]): SinkExecution[A] =
    DataflowExecution.PipeToSink(this, sink)

  def <=>[AA <: A, BB >: B](right: PipeExecution[AA, BB]): PipeExecution[AA, BB] =
    DataflowExecution.ForkJoin(this, right)
}

object DataflowExecution {
  case class PipeToPipe[A, B, C](left: PipeExecution[A, B], right: PipeExecution[B, C]) extends PipeExecution[A, C] {
    override def executionContext = right.executionContext

    override def await(): Unit = {
      left.await()
      right.await()
    }

    override def feedPipe(value: A)(callback: C => Unit): Unit =
      left.feedPipe(value) { v1 =>
        right.feedPipe(v1) { v2 =>
          callback(v2)
        }
      }

    override def statusString = s"${left.statusString} >>> ${right.statusString}"
  }

  case class NamedPipe[A, B](name: String, pipe: PipeExecution[A, B]) extends PipeExecution[A, B] {
    override def executionContext = pipe.executionContext
    override def await() = pipe.await()
    override def statusString = s"${name}:[${pipe.statusString}]"
    override def feed(value: A) = pipe.feed(value)
    override def feedPipe(value: A)(cb: B => Unit) = pipe.feedPipe(value)(cb)
  }

  case class NamedSink[A](name: String, sink: SinkExecution[A]) extends SinkExecution[A] {
    override def await() = sink.await()
    override def statusString = s"${name}:[${sink.statusString}]"
    override def feed(value: A) = sink.feed(value)
  }

  case class PipeToSink[A, B](left: PipeExecution[A, B], right: SinkExecution[B]) extends SinkExecution[A] {
    override def await(): Unit = {
      left.await()
      right.await()
    }

    override def feed(value: A): Unit =
      left.feedPipe(value) { v => right.feed(v) }

    override def statusString = s"${left.statusString} >>~ ${right.statusString}"
  }

  case class ForkJoin[A, B](left: PipeExecution[A, B], right: PipeExecution[A, B]) extends PipeExecution[A, B] {
    private[this] val outBuffer = BufferPipe(1, {v: B => Seq(v) })

    override def executionContext = outBuffer.executionContext

    override def await(): Unit = {
      left.await()
      right.await()
      outBuffer.await()
    }

    override def feedPipe(value: A)(callback: B => Unit): Unit = {
      def feedToOut(b: B): Unit = {
        callback(b)
        outBuffer.feed(b)
      }
      left.feedPipe(value)(feedToOut)
      right.feedPipe(value)(feedToOut)
    }

    override def statusString = s"(${left.statusString()} <=> ${right.statusString()}) >>> ${outBuffer.statusString()}"
  }

  case class ConstantPipe[A, B](threadNum: Int, f: A => Seq[B]) extends PipeExecution[A, B] {
      private[this] val pool = new BlockingThreadPoolExecutor(threadNum, threadNum, 1000, 1)

      override val executionContext = ExecutionContext.fromExecutor(pool)

      override def await(): Unit = {
        pool.shutdown()
        while(!pool.awaitTermination(100, java.util.concurrent.TimeUnit.MILLISECONDS)) ();
      }

      override def feedPipe(value: A)(callback: B => Unit): Unit = {
        Future { f(value).foreach(callback) }(executionContext)
      }

      override def statusString = s"{threads=${pool.getActiveCount}/${pool.getPoolSize}}"
  }

  case class BufferPipe[A, B](size: Int, f: A => Seq[B]) extends PipeExecution[A, B] {
    private[this] val pool = new BlockingThreadPoolExecutor(1, 1, 1000, size)

    private[this] def feedInternal(value: A, callback: B => Unit) =
      pool.execute(new Runnable {
        override def run() {
          f(value).foreach(callback)
        }
      })

    override def executionContext = ExecutionContext.fromExecutor(pool)

    override def await(): Unit = {
      pool.shutdown()
      while(!pool.awaitTermination(100, java.util.concurrent.TimeUnit.MILLISECONDS)) ();
    }

    def sleepUntilEmpty(): Unit = {
      while(pool.getQueue.size() > 0) Thread.sleep(0)
    }

    private[this] val doNothing = { _: B => }

    override def feed(value: A): Unit =
      feedInternal(value, doNothing)

    override def feedPipe(value: A)(callback: B => Unit): Unit =
      feedInternal(value, callback)

    override def statusString = s"buffer(${size - pool.getQueue.remainingCapacity}/${size})"
  }

  case class Serialize[A, B, C](bufferSize: Int, pipe: PipeExecution[A, B], keyOf: A => C) extends PipeExecution[A, B] {
    private[this] val buffer = BufferPipe(bufferSize, {v: A => Seq(v) })
    private[this] var running = scala.collection.mutable.Set.empty[C]
    private[this] val requeueThreads = java.util.concurrent.Executors.newFixedThreadPool(1)

    override def executionContext = pipe.executionContext

    override def await(): Unit = {
      buffer.sleepUntilEmpty()
      requeueThreads.shutdown()
      while(!requeueThreads.awaitTermination(100, java.util.concurrent.TimeUnit.MILLISECONDS)) ();
      buffer.await()
      pipe.await()
    }

    override def feedPipe(value: A)(callback: B => Unit): Unit = {
      val k = keyOf(value)
      buffer.feedPipe(value) { v =>
        val go = synchronized {
          if(!running.contains(k)) {
            running += k
            true
          } else {
            false
          }
        }

        if(go) {
          pipe.feedPipe(v) { result =>
            synchronized { running -= k }
            callback(result)
          }
        } else {
          requeueThreads.execute(new Runnable {
            override def run(): Unit = {
              feedPipe(v)(callback)
            }
          })
        }
      }
    }

    override def statusString = s"serialized(buffer={${buffer.statusString()}}, executor={${pipe.statusString()}})"
  }
}
