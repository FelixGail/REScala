package tests.rescala.fullmv

import org.scalatest.FunSuite
import rescala.core.{Pulse, ValuePersistency}
import rescala.fullmv.FramingBranchResult.{Deframe, Frame, FramingBranchEnd}
import rescala.fullmv.NotificationResultAction.NotificationOutAndSuccessorOperation.FollowFraming
import rescala.fullmv._

import scala.concurrent.duration.Duration

class NodeVersionHistoryTest extends FunSuite {
  test("SupersedeFraming into double marker trailer") {
    val engine = new FullMVEngine(Duration.Zero, "asd")

    val createN = engine.newTurn()
    createN.beginExecuting()
    val n = new NodeVersionHistory[Pulse[Int], FullMVTurn, Int, Int](createN, ValuePersistency.InitializedSignal(Pulse.Value(10)))
    createN.completeExecuting()

    val reevaluate = engine.newTurn()
    reevaluate.beginFraming()
    assert(n.incrementFrame(reevaluate) === Frame(Set.empty, reevaluate))
    reevaluate.completeFraming()

    val framing1 = engine.newTurn()
    framing1.beginFraming()
    val framing2 = engine.newTurn()
    framing2.beginFraming()
    val lock = SerializationGraphTracking.tryLock(framing1, framing2, UnlockedUnknown) match {
      case x@LockedSameSCC(_) => x
      case other => fail("not locked: " + other)
    }
    framing2.addPredecessor(framing1.selfNode)
    lock.unlock()

    assert(n.incrementFrame(framing2) === FramingBranchEnd) // End because earlier frame by reevaluate turn exists

    n.notify(reevaluate, changed = true)
    n.retrofitSinkFrames(Seq.empty, Some(framing1), -1)
    assert(n.reevOut(reevaluate, Some(Pulse.Value(11))) === FollowFraming(Set.empty, framing2))

    assert(n.incrementSupersedeFrame(framing1, framing2) === Deframe(Set.empty, framing2))
  }
}
