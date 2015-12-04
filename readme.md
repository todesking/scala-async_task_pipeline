# AsyncTaskPipeline

## Description

AsyncTaskPipeline is framework for building in-process parallel task processing pipeline.

```scala
resolvers += "com.todesking" at "http://todesking.github.io/mvn/"

libraryDependencies += "com.todesking" %% "async_task_pipeline" % "0.0.9"
```

## Usage

1. Build pipes/sink with `Dataflow.build*`
2. Join pipes.
3. `Dataflow.runPipe/runSink` and get Execution object.
4. `feed()` values to execution context.

```scala
import com.todesking.{async_task_pipeline => ap}

import ap.Dataflow.{buildPipe, buildSinkToGrowable}
import ap.Parallelism

val par = Parallelism.Const(100) // concurrency = 100

val output = new scala.collection.mutable.ArrayBuffer[Int]

val context = ap.Dataflow.runSink(
  buildPipe(par) {i: Int => if(i % 2 == 0) Seq(i) else Seq.empty }
  >>>
  buildPipe(par) {i: Int => Seq(i * 2)}
  >>~
  buildSinkToGrowable(output)
)

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
object Dataflow {
  def buildPipe[A, B](par: Parallelism)(f: A => Seq[B]): Pipe[A, B]
  def buildSink[A, B](par: Parallelism)(f: A => Unit): Sink[A]
  def buildSinkToGrowable[A](g: Growable): Sink[A]

  def runPipe[A, B](pipe: Pipe[A, B]): PipeExecution[A, B]
  def runSink[A](sink: Sink[A]): Sink[A]
}

trait Dataflow
trait Sink[-A] extends Dataflow
trait Pipe[-A, +B] extends Dataflow with Sink[A] {
  def >>>[C](rhs: Pipe[B, C]): Pipe[A, C]
  def >>~(rhs: Sink[B]): Sink[A]
  def <=>(rhs: Pipe[A, B]): Pipe[A, B]
}

trait DataflowExecution {
  def await(): Unit
}
trait SinkExecution[-A] extends DataflowExecution {
  def feed(value: A): Unit
}
trait PipeExecution[-A, +B] extends DataflowExecution with SinkExecution[A] {
  def feedPipe(value: A)(callback: Seq[B] => Unit): Unit
  def feedPipe1(value: A)(callback: B => Unit): Unit
}
```

## TODO

* Error handling
* Througput in statusString
