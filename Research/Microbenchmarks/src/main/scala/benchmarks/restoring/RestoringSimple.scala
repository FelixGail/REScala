package benchmarks.restoring

import java.util.concurrent.TimeUnit

import benchmarks.{EngineParam, Size, Step}
import org.openjdk.jmh.annotations._
import rescala.core.{Engine, Struct}
import rescala.reactives.{Evt, Var}
import rescala.restore.ReStoringEngine
import rescala.core.ReCirce._

@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 3, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Fork(3)
@Threads(1)
@State(Scope.Thread)
class RestoringSimple[S <: Struct] {

  implicit var engine: Engine[S] = _

  var source: Evt[Int, S] = _
  var result: List[Any] = _

  @Setup
  def setup(size: Size, engineParam: EngineParam[S]) = {
    engine = engineParam.engine
    source = engine.Evt[Int]()
    result = Nil
    if (size.size <= 0) result = List(source.map(_+1))
    for (_ <- Range(0, size.size)) {
      result = source.count :: result
    }
  }

  @Benchmark
  def countMany(step: Step): Unit = source.fire(step.run())


}

@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 3, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Fork(3)
@Threads(1)
@State(Scope.Thread)
class RestoringVar[S <: Struct] {

  implicit var engine: Engine[S] = _
  var sourceVar: Var[Int, S] = _

  @Setup
  def setup(engineParam: EngineParam[S]) = {
    engine = engineParam.engine
    sourceVar = engine.Var(-1)
  }

  @Benchmark
  def singleVar(step: Step): Unit = sourceVar() = step.run()
}

@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 3, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Fork(3)
@Threads(1)
@State(Scope.Thread)
class RestoringSnapshot[S <: Struct] {

  var snapshot: Seq[(String, String)] = _

  def build(implicit engine: ReStoringEngine, size: Int) = {
    val source = engine.Evt[Int]()
    val res = for (i <- 1 to size) yield {
      source.count.map(_+1).map(_+1)
    }
    (source, res)
  }

  @Setup
  def setup(size: Size) = {
    val engine = new ReStoringEngine()
    val (source, res) = build(engine, size.size)
    source.fire(10)(engine)
    source.fire(20)(engine)
    snapshot = engine.snapshot().toSeq
  }

  @Benchmark
  def fresh(size: Size): Unit = build(new ReStoringEngine(), size.size)

  @Benchmark
  def restored(size: Size): Unit = {
    val engine = new ReStoringEngine(restoreFrom = snapshot)
    build(engine, size.size)
  }


}
