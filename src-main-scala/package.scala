package com.todesking.async_task_pipeline

import java.util.{concurrent => jc}
import scala.collection.{mutable => mutable}

object AsyncTaskPipeline {
  val builder = Builder
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

