package com.todesking.async_task_pipeline

object Builder {
  def pipe[A, B]() = new PipeB[A, B]

  def sinkToGrowable[A](dest:scala.collection.generic.Growable[A]) = new SinkToGrowable[A](dest)

  class PipeB[A, B] {
    def unordered(thc:ThreadPoolConfig)(proc:A => Option[B]):UnorderedPipeImpl[A, B] = {
      new UnorderedPipeImpl[A, B](proc, thc)
    }
    def unordered(proc:A => Option[B]):UnorderedPipeImpl[A, B] = unordered(ThreadPoolConfig.default())(proc)

    def unordered() = new UnorderedB
    class UnorderedB {
      def unique[G](thc:ThreadPoolConfig)(groupOf:A => G)(proc:A => Option[B]) = new UnorderedUniquePipeImpl[A, B, G](
        groupOf, proc,
        10,
        thc
      )
      def unique[G](groupOf:A => G)(proc:A => Option[B]):UnorderedUniquePipeImpl[A, B, G] =
        unique[G](ThreadPoolConfig.default())(groupOf)(proc)

      def bufferedUnique[G](thc:ThreadPoolConfig)(bufferSize:Int)(groupOf:A => G)(proc:A => Option[B]):Pipe[A, B] =
        new UnorderedUniqueBufferedPipeImpl[A, B, G](groupOf, proc, thc, bufferSize)

      def bufferedUnique[G](bufferSize:Int)(groupOf:A => G)(proc: A => Option[B]):Pipe[A, B] = bufferedUnique[G](ThreadPoolConfig.default())(bufferSize)(groupOf)(proc)
    }
  }
}
