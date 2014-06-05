# AsyncTaskPipeline

## Description

AsyncTaskPipeline is framework for building in-process parallel task processing pipeline.

```scala
resolvers += "com.todesking" at "http://todesking.github.io/mvn/"

libraryDependencies += "com.todesking" %% "async_task_pipeline" % "0.0.1"
```

## Usage

1. Build pipes with `AsyncTaskPipeline.builder`
2. Join pipes.
3. `run()` and get execution context.
4. `feed()` values to execution context.

```scala
import com.todesking.{async_task_pipeline => ap}

val builder = ap.AsyncTaskPipeline.builder

val output = new scala.collection.mutable.ArrayBuffer[Int]

val context = (
  builder.pipe[Int, Int].unordered {i => if(i % 2 == 0) Some(i) else None }
  >>
  builder.pipe[Int, Int].unordered {i => Some(i * 2)}
  >>
  builder.sinkToGrowable(output)
).run()

context.feed(1)
context.feed(2)
context.feed(3)
context.feed(4)

// Wait until all process completed.
context.await()

output.sorted == Seq(4, 8)
```

## Architecture

```scala
trait Closure {
  def run():ExecutionContext
}

trait ExecutionContext {
  def await():Unit
}

trait Source[A] extends Closure {
  def >>(sink:Sink[A]):Closure
  def >>[B](pipe:Pipe[A, B]):Source[B]
  override def run():SourceExecutionContext[A]
}
trait SourceExecutionContext[A] extends ExecutionContext {
  def wireTo(sink:SinkExecutionContext[A]):Unit
}

Sink[A] extends Closure {
  override def run():SinkExecutionContext[A]
}
trait SinkExecutionContext[A] extends ExecutionContext {
  def feed(value:A):Unit
}


trait PipeExecutionContext[A, B] extends ExecutionContext with SinkExecutionContext[A] with SourceExecutionContext[B]
trait Pipe[A, B] extends Closure with Sink[A] with Source[B] {
  override def run():PipeExecutionContext[A, B]
}
```
