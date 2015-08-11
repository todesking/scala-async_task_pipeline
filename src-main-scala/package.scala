package com.todesking.async_task_pipeline

import java.util.{concurrent => jc}
import scala.collection.{mutable => mutable}

object AsyncTaskPipeline {
  val builder = Builder
}

trait ExecutionContext {
  // Close pipe and await all process finished
  def await():Unit

  def statusMessage():String = s"(${toString})"

  def getSink[A](name: String): SinkExecutionContext[A] =
    elements.filter(_._1 == name).head._2.asInstanceOf[SinkExecutionContext[A]]

  def elements: Seq[(String, ExecutionContext)]
}

object ExecutionContext {
  def nullSink[A]():SinkExecutionContext[A] =
    new SinkExecutionContext[A]() {
      override def feed(value:A):Unit = ()
      override def await():Unit = ()
      override def elements = Seq.empty
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
  def |(other: Pipe[A, B]): Pipe[A, B] = new ParallelPipe[A, B](this, other)
  override def run():PipeExecutionContext[A, B]
}

