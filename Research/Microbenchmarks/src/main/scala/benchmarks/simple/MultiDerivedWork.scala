package benchmarks.simple

import java.util.concurrent.{ThreadLocalRandom, TimeUnit}

import benchmarks._
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.BenchmarkParams
import rescala.core.{ReSource, Scheduler, Struct}
import rescala.reactives.{Signal, Var}

@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 3, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Fork(1)
@Threads(1)
@State(Scope.Benchmark)
class MultiDerivedWork[S <: Struct] extends BusyThreads {
  implicit var engine: Scheduler[S] = _
  var sources: Set[Var[Int, S]] = _
  var maps: Iterable[Signal[Int, S]] = _
  @Param(Array("1", "4", "16"))
  var width: Int = _
  @Param(Array("1", "4", "16"))
  var depth: Int = _
  @Setup(Level.Iteration)
  def setup(params: BenchmarkParams, engineParam: EngineParam[S], size: Size, work: Workload) = {
    engine = engineParam.engine
    sources = Array.fill(size.size)(Var(0)).toSet
    maps = sources.map(_.map{ v => work.consume(); v + 1 })
  }

  @Benchmark
  def run(): Unit = {
    engine.executeTurn(sources.asInstanceOf[Set[ReSource[S]]], { t =>
      sources.foreach{_.admit(ThreadLocalRandom.current().nextInt())(t)}
    })
  }
}
