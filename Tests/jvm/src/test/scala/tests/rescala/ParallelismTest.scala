package tests.rescala

import org.scalatest.FunSuite
import rescala.testhelper.{ReevaluationTracker, Spawn}
import rescala.engine.{Engine, Turn}
import rescala.fullmv.FullMVEngine
import rescala.graph.Struct

import scala.collection.mutable.ArrayBuffer

class ParallelismTest extends FunSuite {
  val delay = 50L
  type S <: Struct

  rescala.testhelper.TestEngines.all.foreach { asd =>
    val engine = asd.asInstanceOf[Engine[S, Turn[S]]]
    import engine._

    (if(engine == FullMVEngine) test _ else ignore _)(s"in-turn parallelism is supported by $engine", Seq()) {
      object lock
      val theLog = new ArrayBuffer[Symbol](4)

      def log(symbol: Symbol): Unit = lock.synchronized {
        theLog += symbol
      }

      val input = Evt[Unit]()
      val leftBranch = new ReevaluationTracker[Unit, S](input.map { x =>
        log('enterLeft)
        Thread.sleep(delay)
        log('exitLeft)
        x
      })
      val rightBranch = new ReevaluationTracker[Unit, S](input.map { x =>
        log('enterRight)
        Thread.sleep(delay)
        log('exitRight)
        x
      })

      input.fire()

      leftBranch.assertClear(Unit)
      rightBranch.assertClear(Unit)

      assert(theLog.indexOf('enterLeft) < theLog.indexOf('exitRight), ": engine forced left branch to execute after right branch")
      assert(theLog.indexOf('enterRight) < theLog.indexOf('exitLeft), ": engine forced right branch to execute after left branch")
    }

    (if(engine == FullMVEngine) test _ else ignore _)(s"pipeline parallelism is supported by $engine", Seq()) {
      object lock
      val theLog = new ArrayBuffer[(String, Symbol)](4)

      def log(updateId: String, symbol: Symbol): Unit = lock.synchronized {
        theLog += ((updateId, symbol))
      }

      val input = Evt[String]()
      val chain = new ReevaluationTracker[String, S](input.map { x =>
        log(x, 'enterFirst)
        Thread.sleep(delay)
        log(x, 'exitFirst)
        x
      }.map { x =>
        log(x, 'enterSecond)
        Thread.sleep(delay)
        log(x, 'exitSecond)
        x
      })

      val worker = Spawn( input("spawned") )
      input.fire("main")
      worker.join(5 * delay)

      assert(List("main", "spawned").permutations.contains(chain.results), " - Propagation Error or Serializability Violation!")

      // whatever serialization order was chosen..
      val firstUpdateId = theLog.head._1
      val secondUpdateId = theLog.last._1
      assert(firstUpdateId != secondUpdateId, "serializability broke")

      assert(theLog.indexOf((secondUpdateId, 'enterFirst)) < theLog.indexOf((firstUpdateId, 'exitSecond)), ": engine forced second thread to reevaluate first node after first thread reevaluated second node")
      assert(theLog.indexOf((firstUpdateId, 'enterSecond)) < theLog.indexOf((secondUpdateId, 'exitFirst)), ": engine forced first thread to reevaluate second node after second thread reevaluated first node")
    }
  }
}
