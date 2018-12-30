package rescala.benchmarks.distributed.rtt

import java.util.concurrent._

import org.openjdk.jmh.annotations._
import rescala.core.REName
import rescala.fullmv.mirrors.localcloning.{FakeDelayer, ReactiveLocalClone}
import rescala.fullmv.{FullMVEngine, FullMVStruct}
import rescala.reactives.{Signal, Var}

import scala.concurrent.duration._

@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5, time = 5000, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 7, time = 5000, timeUnit = TimeUnit.MILLISECONDS)
@Fork(1)
@Threads(1)
@State(Scope.Benchmark)
class Remerge {
  @Param(Array("5"))
  var totalLength: Int = _

  @Param(Array("2"))
  var widthHosts: Int = _

  @Param(Array("1", "2", "3", "5"))
  var mergeAt: Int = _

  var sourceHost: FullMVEngine = _
  var source: Var[Int, FullMVStruct] = _
  var preMergeDistance: Seq[Seq[(FullMVEngine, Signal[Int, FullMVStruct])]] = _
  var mergeHost: FullMVEngine = _
  var remotesOnMerge: Seq[Signal[Int, FullMVStruct]] = _
  var merge: Signal[Int, FullMVStruct] = _
  var postMergeDistance: Seq[(FullMVEngine, Signal[Int, FullMVStruct])] = _

  @Param(Array("50"))
  var msDelay: Int = _

  @Setup(Level.Iteration)
  def setup(): Unit = {
    FakeDelayer.enable()

    sourceHost = new FullMVEngine(10.seconds, s"src")
    source = {
      val engine = sourceHost
      import engine._
      engine.Var(0)
    }

    var preMerge: Seq[(FullMVEngine, Signal[Int, FullMVStruct])] = Seq.fill(widthHosts)(sourceHost -> source)
    preMergeDistance = for (d <- 1 until mergeAt) yield {
      preMerge = for (i <- 1 to widthHosts) yield {
        val host = new FullMVEngine(10.seconds, s"premerge-$d-$i")
        host -> REName.named(s"clone-premerge-$d-$i") { implicit ! =>
          ReactiveLocalClone(preMerge(i - 1)._2, host, msDelay.millis)
        }
      }
      preMerge
    }

    mergeHost = new FullMVEngine(10.seconds, "merge")
    remotesOnMerge = for (i <- 1 to widthHosts) yield {
      REName.named(s"clone-merge-$i") { implicit ! =>
        ReactiveLocalClone(preMerge(i - 1)._2, mergeHost, msDelay.millis)
      }
    }
    merge = {
      val e = mergeHost
      import e._
      Signals.static(remotesOnMerge:_*) { t =>
        remotesOnMerge.map(t.dependStatic).sum
      }
    }

    var postMerge: (FullMVEngine, Signal[Int, FullMVStruct]) = mergeHost -> merge
    postMergeDistance = for (i <- 1 to totalLength - mergeAt) yield {
      val host = new FullMVEngine(10.seconds, s"host-$i")
      postMerge = host -> REName.named(s"clone-$i") { implicit ! =>
        ReactiveLocalClone(postMerge._2, host, msDelay.millis)
      }
      postMerge
    }

  }

  @TearDown(Level.Iteration)
  def teardown(): Unit = {
    FakeDelayer.shutdown()
  }

  @Benchmark
  def run(): Unit = {
    source.transform(_ + 1)(sourceHost)
  }
}
