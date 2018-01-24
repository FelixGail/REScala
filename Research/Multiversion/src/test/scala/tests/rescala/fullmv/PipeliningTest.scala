package tests.rescala.fullmv

import org.scalatest.FunSuite
import rescala.fullmv.FullMVEngine.default._
import tests.rescala.util.Spawn

class PipeliningTest extends FunSuite {
  test("pipelining works") {
    val millisecondsPerNode = 10
    val pipelineLength = 20
    val numberOfUpdates = 10

    val input = Var(0)
    val derived: Array[Signal[Int]] = new Array(pipelineLength)
    for(i <- 0 until pipelineLength) {
      val from = if(i == 0) input else derived(i - 1)
      derived(i) = from.map { v =>
        Thread.sleep(millisecondsPerNode)
        v + 1
      }
    }
    var all = Seq.empty[Int]
    derived(derived.length - 1).observe(all :+= _)

    val leastPossibleMillisecondsWithoutPipelining = pipelineLength * numberOfUpdates * millisecondsPerNode

    val startTime = System.currentTimeMillis()
    val spawned = for (_ <- 1 to numberOfUpdates) yield Spawn(input.transform(_ + 1))
    val timeout = System.currentTimeMillis() + leastPossibleMillisecondsWithoutPipelining + 1000
    spawned.foreach(_.join(math.max(0, timeout - System.currentTimeMillis())))
    val endTime = System.currentTimeMillis()

    assert(all === (pipelineLength to (pipelineLength + numberOfUpdates)))
    assert(endTime - startTime < leastPossibleMillisecondsWithoutPipelining)
  }
}
