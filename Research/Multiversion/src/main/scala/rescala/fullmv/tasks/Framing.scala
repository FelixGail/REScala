package rescala.fullmv.tasks

import java.util.concurrent.locks.LockSupport

import rescala.core.ReSource
import rescala.fullmv.FramingBranchResult._
import rescala.fullmv._

trait FramingTask extends FullMVAction {
  override def doCompute(): Unit = {
    val branchResult = doFraming()
    if(FullMVEngine.DEBUG) println(s"[${Thread.currentThread().getName}] $this => $branchResult")
    branchResult match {
      case FramingBranchEnd =>
        Traversable.empty
      case Frame(out, maybeOtherTurn) =>
        if(maybeOtherTurn != turn) {
          for(dep <- out) maybeOtherTurn.pushExternalTask(Framing(maybeOtherTurn, dep))
          LockSupport.unpark(maybeOtherTurn.userlandThread)
        } else {
          for(dep <- out) maybeOtherTurn.pushLocalTask(Framing(maybeOtherTurn, dep))
        }
      case FrameSupersede(out, maybeOtherTurn, supersede) =>
        if(maybeOtherTurn != turn) {
          for(dep <- out) maybeOtherTurn.pushExternalTask(SupersedeFraming(maybeOtherTurn, dep, supersede))
          LockSupport.unpark(maybeOtherTurn.userlandThread)
        } else {
          for(dep <- out) maybeOtherTurn.pushLocalTask(SupersedeFraming(maybeOtherTurn, dep, supersede))
        }
    }
  }

  def doFraming(): FramingBranchResult[FullMVTurn, ReSource[FullMVStruct]]
}

case class Framing(turn: FullMVTurn, node: ReSource[FullMVStruct]) extends FramingTask {
  override def doFraming(): FramingBranchResult[FullMVTurn, ReSource[FullMVStruct]] = {
    assert(turn.phase == TurnPhase.Framing, s"$this cannot increment frame (requires framing phase)")
    node.state.incrementFrame(turn)
  }
}

case class SupersedeFraming(turn: FullMVTurn, node: ReSource[FullMVStruct], supersede: FullMVTurn) extends FramingTask {
  override def doFraming(): FramingBranchResult[FullMVTurn, ReSource[FullMVStruct]] = {
    assert(turn.phase == TurnPhase.Framing, s"$this cannot increment frame (requires framing phase)")
    assert(supersede.phase == TurnPhase.Framing, s"$supersede cannot have frame superseded (requires framing phase)")
    node.state.incrementSupersedeFrame(turn, supersede)
  }
}
