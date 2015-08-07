import org.scalatest._

import scala.collection.{mutable => mutable}

import com.todesking.{async_task_pipeline => ap}


class Spec extends FlatSpec with Matchers {
  val builder = ap.AsyncTaskPipeline.builder

  "SinkToGrowable[A]" should "store feeded values to given Growable" in {
    val results = new mutable.ArrayBuffer[Int]
    val ctx = builder.sinkToGrowable(results).run()

    results should be (Seq())

    ctx.feed(1)
    ctx.feed(2)
    ctx.feed(3)
    println(ctx.statusMessage)
    ctx.await()

    results should be(Seq(1, 2, 3))
  }

  "UnorderedPipeImpl[A]" should "process values" in {
    val results = new mutable.ArrayBuffer[Int]
    val sink = builder.sinkToGrowable(results)

    val ctx = (builder.pipe[Int, Int].unordered {i => if(i % 2 == 0) Some(i) else None } >> sink).run()

    (1 to 2000).foreach(ctx.feed(_))
    println(ctx.statusMessage)
    ctx.await()

    results.sorted should be((1 to 2000).filter(_ % 2 == 0))
  }

  "UnorderedUniquePipeImpl[A]" should "process values" in {
    val results = new mutable.ArrayBuffer[Int]
    val sink = builder.sinkToGrowable(results)

    val ctx = (builder.pipe[Int, Int].unordered.unique(i => i / 3) {i => if(i % 2 == 0) Some(i) else None }.named("even") >> sink).run()

    (1 to 2000).foreach(ctx.feed(_))
    println(ctx.statusMessage)
    ctx.await()

    results.sorted should be((1 to 2000).filter(_ % 2 == 0))
  }

  "UnorderedUniqueBufferedPipeImpl[A]" should "process values" in {
    val results = new mutable.ArrayBuffer[Int]
    val sink = builder.sinkToGrowable(results)

    val ctx = (builder.pipe[Int, Int].unordered.bufferedUnique(10)(i => i / 3) {i => if(i % 2 == 0) Some(i) else None } >> sink).run()

    (1 to 2000).foreach(ctx.feed(_))
    println(ctx.statusMessage)
    ctx.await()

    results.sorted should be((1 to 2000).filter(_ % 2 == 0))
  }

  "ParallelPipe[A, B]" should "process values" in {
    val results = new mutable.ArrayBuffer[Int]
    val ctx = (
      (builder.pipe[Int, Int].unordered(i => Some(i * 10)) | builder.pipe[Int, Int].unordered(i => Some(i * 100))) >> builder.sinkToGrowable(results)
    ).run()

    ctx.feed(1)
    ctx.feed(2)
    ctx.await()

    results.sorted should be(Seq(10, 20, 100, 200))
  }

  "Complex pipeline" should "process values" in {
    val results = new mutable.ArrayBuffer[Int]
    val sink = builder.sinkToGrowable(results)

    val ctx = (
      builder.pipe[String, Int].unordered(i => try {Some(i.toInt)} catch { case _:NumberFormatException => None })
      >>
      builder.pipe[Int, String].unordered(i => Some(i.toString))
      >>
      builder.pipe[String, Int].unordered(i => Some(i.toInt))
      >>
      builder.pipe[Int, Int].unordered.unique(_ % 2) {i => Some(i * 2)}
      >>
      sink
    ).run()

    (1 to 1000).foreach(i => ctx.feed(i.toString))
    Seq("a", "b", "c").foreach(ctx.feed(_))
    (1001 to 2000).foreach(i => ctx.feed(i.toString))
    println(ctx.statusMessage)
    ctx.await()

    results.sorted should be((1 to 2000).map(_ * 2))
  }
}
