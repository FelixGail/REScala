package benchmarks.simple

import java.util.concurrent.TimeUnit

import benchmarks.{EngineParam, Size, Step, Workload}
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.BenchmarkParams
import rescala.core.{Scheduler, Struct}
import rescala.reactives._

@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 3, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Fork(3)
@Threads(1)
@State(Scope.Thread)
class Fan[S <: Struct] {

  implicit var engine: Scheduler[S] = _

  var source: Var[Int, S] = _
  var res: Seq[Signal[Int, S]] = _

  @Setup
  def setup(params: BenchmarkParams, size: Size, work: Workload, step: Step, engineParam: EngineParam[S]) = {
    engine = engineParam.engine
    source = Var(step.run())
    res = for (_ <- 1 to size.size) yield {
      source.map { x =>
        work.consume()
        x + 1
      }
    }
//    result = Signals.lift(res) { _.sum }
  }

  @Benchmark
  def run(step: Step): Unit = source.set(step.run())
}
