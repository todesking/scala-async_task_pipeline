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
    ctx.await()

    results should be(Seq(1, 2, 3))
  }

  "UnorderedPipeImpl[A]" should "process values" in {
    val results = new mutable.ArrayBuffer[Int]
    val sink = builder.sinkToGrowable(results)

    val ctx = (builder.pipe[Int, Int].unordered {i => if(i % 2 == 0) Some(i) else None } >> sink).run()

    (1 to 1000).foreach(ctx.feed(_))
    ctx.await()

    results.sorted should be((1 to 1000).filter(_ % 2 == 0))
  }
}
