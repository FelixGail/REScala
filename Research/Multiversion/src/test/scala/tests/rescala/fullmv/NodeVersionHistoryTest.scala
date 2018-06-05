package tests.rescala.fullmv

import org.scalatest.FunSuite
import rescala.core.Initializer
import rescala.fullmv.FramingBranchResult.{Frame, FramingBranchEnd}
import rescala.fullmv.NotificationResultAction.{ChangedSomethingInQueue, GlitchFreeReady, NotGlitchFreeReady}
import rescala.fullmv.NotificationResultAction.NotificationOutAndSuccessorOperation.{NextReevaluation, NoSuccessor}
import rescala.fullmv._

class NodeVersionHistoryTest extends FunSuite {
  test("Frame Notify Reevout") {
    val engine = new FullMVEngine("fnr")

    val createN = engine.newTurn()
    createN.beginExecuting()
    val n = new NonblockingSkipListVersionHistory[Int, FullMVTurn, Int, Int](createN, Initializer.InitializedSignal(10))
    createN.completeExecuting()

    val turn1 = engine.newTurn()
    turn1.beginFraming()
    assert(n.incrementFrame(turn1) === Frame(Set.empty, turn1))
    assert(n.incrementFrame(turn1) === FramingBranchEnd)
    turn1.completeFraming()
    assert(n.notify(turn1, changed = true) === NotGlitchFreeReady)
    assert(n.notify(turn1, changed = false) === GlitchFreeReady)
    assert(n.reevIn(turn1) === 10)
    assert(n.reevOut(turn1, Some(5)) === NoSuccessor(Set.empty))
    turn1.completeExecuting()

    val turn2 = engine.newTurn()
    turn2.beginFraming()
    assert(n.incrementFrame(turn2) === Frame(Set.empty, turn2))
    turn2.completeFraming()
    assert(n.notify(turn2, changed = true) === GlitchFreeReady)
    assert(n.reevIn(turn2) === 5)
    assert(n.reevOut(turn2, Some(10)) === NoSuccessor(Set.empty))
    turn2.completeExecuting()
  }

  test("Unchanged") {
    val engine = new FullMVEngine("nochange")

    val createN = engine.newTurn()
    createN.beginExecuting()
    val n = new NonblockingSkipListVersionHistory[Int, FullMVTurn, Int, Int](createN, Initializer.InitializedSignal(10))
    createN.completeExecuting()

    val turn1 = engine.newTurn()
    turn1.beginFraming()
    assert(n.incrementFrame(turn1) === Frame(Set.empty, turn1))
    turn1.completeFraming()
    assert(n.notify(turn1, changed = false) === NoSuccessor(Set.empty))
    turn1.completeExecuting()

    val turn2 = engine.newTurn()
    turn2.beginFraming()
    assert(n.incrementFrame(turn2) === Frame(Set.empty, turn2))
    turn2.completeFraming()

    val turn3 = engine.newTurn()
    turn3.beginFraming()
    assert(n.incrementFrame(turn3) === FramingBranchEnd)
    turn3.completeFraming()

    assert(n.notify(turn3, changed = true) === ChangedSomethingInQueue)
    assert(n.notify(turn2, changed = false) === NextReevaluation(Set.empty, turn3))
    assert(n.reevIn(turn3) === 10)
    assert(n.reevOut(turn3, Some(5)) === NoSuccessor(Set.empty))
    turn2.completeExecuting()
  }

  test("SupersedeFraming into double marker trailer") {
    val engine = new FullMVEngine("supersede")

    val createN = engine.newTurn()
    createN.beginExecuting()
    val n = new NonblockingSkipListVersionHistory[Int, FullMVTurn, Int, Int](createN, Initializer.InitializedSignal(10))
    createN.completeExecuting()

    val reevaluate = engine.newTurn()
    reevaluate.beginFraming()
    assert(n.incrementFrame(reevaluate) === Frame(Set.empty, reevaluate))
    reevaluate.completeFraming()

    val framing1 = engine.newTurn()
    framing1.beginFraming()
    val framing2 = engine.newTurn()
    framing2.beginFraming()
    val lock = SerializationGraphTracking.tryLock(framing1, framing2, UnlockedUnknown).asInstanceOf[LockedSameSCC].lock
    try {
      framing2.addPredecessor(framing1.selfNode)
    } finally {
      lock.unlock()
    }

    assert(n.incrementFrame(framing2) === FramingBranchEnd) // End because earlier frame by reevaluate turn exists

    n.notify(reevaluate, changed = true)
    n.retrofitSinkFrames(Nil, Some(framing1), -1)
    assert(n.reevOut(reevaluate, Some(11)) === NoSuccessor(Set.empty))
//    assert(n.reevOut(reevaluate, Some(Pulse.Value(11))) === FollowFraming(Set.empty, framing2))

    assert(n.incrementSupersedeFrame(framing1, framing2) === FramingBranchEnd)
//    assert(n.incrementSupersedeFrame(framing1, framing2) === Deframe(Set.empty, framing2))
  }
}
