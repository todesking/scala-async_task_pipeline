import org.scalatest._

import scala.collection.{mutable => mutable}

import com.todesking.{async_task_pipeline => ap}


class Spec extends FlatSpec with Matchers {
  import ap.Dataflow.{runSink, runPipe, serialize, buildPipe, buildSink, buildSinkToGrowable}

  "SinkToGrowable[A]" should "store feeded values to given Growable" in {
    val results = new mutable.ArrayBuffer[Int]
    val ctx = runSink(buildSinkToGrowable(results))

    results should be (Seq())

    ctx.feed(1)
    ctx.feed(2)
    ctx.feed(3)
    println(ctx.statusString)
    ctx.await()

    results should be(Seq(1, 2, 3))
  }


  "Pipe" should "process values" in {
    val results = new mutable.ArrayBuffer[Int]
    val sink = buildSinkToGrowable(results)
    val par = ap.Parallelism.Constant(100)

    val ctx = runSink(buildPipe[Int, Int](par){i => if(i % 2 == 0) Seq(i) else Seq.empty }.named("even") >>~ sink)

    (1 to 2000).foreach(ctx.feed(_))
    println(ctx.statusString)
    ctx.await()

    results.sorted should be((1 to 2000).filter(_ % 2 == 0))
  }

  "Parallel pipe" should "process values" in {
    val results = new mutable.ArrayBuffer[Int]
    val par = ap.Parallelism.Constant(100)
    val ctx = runSink(
      (buildPipe(par) { i: Int => Seq(i * 10) } <=> buildPipe(par){i: Int => Seq(i * 100)}) >>~ buildSinkToGrowable(results)
    )

    ctx.feed(1)
    ctx.feed(2)
    ctx.await()

    results.sorted should be(Seq(10, 20, 100, 200))
  }

  "cuncurrency limit pipe" should "process values" in {
    val results = new mutable.ArrayBuffer[Int]
    val par = ap.Parallelism.Constant(100)
    val ctx = runSink(
      serialize(100){ i: Int => i % 2 }(
        buildPipe(par){i: Int => Seq(i + 1)}
      ) >>~ buildSinkToGrowable(results)
    )

    (0 to 1000) foreach { i => ctx.feed(i) }
    Thread.sleep(1)
    println(ctx.statusString)
    ctx.await()

    results.sorted should be(1 to 1001)
  }

  "concurrency limit" should "works well" in {
    val par = ap.Parallelism.Constant(1000)
    var a: Int = 0
    var b: Int = 0
    val ctx = runSink(
      serialize(100) { i: Int => i % 2 } (
        buildPipe(par){i: Int => if(i % 2 == 0) { val aa = a; Thread.sleep(0); a = aa + 1 } else { val bb = b; Thread.sleep(0); b = bb + 1 }; Seq.empty }
      )
    )
    (1 to 10000).foreach { i => ctx.feed(i) }
    ctx.await()

    a should be(5000)
    b should be(5000)
  }

  "Complex pipeline" should "process values" in {
    val results = new mutable.ArrayBuffer[Int]
    val sink = buildSinkToGrowable(results)
    val par = ap.Parallelism.Constant(100)

    var a: Int = 0
    var b: Int = 0

    val ctx = runSink(
      buildPipe[String, Int](par)(i => try {Seq(i.toInt)} catch { case _:NumberFormatException => Seq.empty })
      >>>
      buildPipe[Int, String](par)(i => Seq(i.toString))
      >>>
      buildPipe[String, Int](par)(i => Seq(i.toInt))
      >>>
      serialize(100) { i: Int => i % 2 == 0 }(
        buildPipe(par){i: Int => if(i % 2 == 0) { val aa = a; Thread.sleep(0); a = aa + 1 } else { val bb = b; Thread.sleep(0); b = bb + 1 }; Seq(i) }
        >>>
        buildPipe[Int, Int](par) {i => Seq(i * 2)}
      )
      >>~
      sink
    )

    (1 to 5000).foreach(i => ctx.feed(i.toString))
    Seq("a", "b", "c").foreach(ctx.feed(_))
    (5001 to 10000).foreach(i => ctx.feed(i.toString))
    println(ctx.statusString)
    ctx.await()

    results.sorted should be((1 to 10000).map(_ * 2))
    a should be(5000)
    b should be(5000)
  }

  "DataflowExecution.await" should "rethrow error from pipe execution" in {
    val par = ap.Parallelism.Constant(100)
    val ctx = runSink(
      buildPipe(par) { i: Int => throw new RuntimeException(i.toString) }
    )

    ctx.feed(999)

    the[RuntimeException] thrownBy { ctx.await() } should have message("999")
  }
}
