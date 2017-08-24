package rescala.fullmv.mirrors.localcloning

import rescala.fullmv.mirrors._
import rescala.fullmv.transmitter.ReactiveTransmittable
import rescala.fullmv.{FullMVEngine, FullMVTurn, TransactionSpanningTreeNode, TurnPhase}

import scala.concurrent.Future

object FullMVTurnLocalClone {
  def apply(turn: FullMVTurn, reflectionHost: FullMVEngine): FullMVTurn = {
    reflectionHost.getCachedOrReceiveRemote(turn.guid) { cacheNow =>
      val mirrorHost = turn.host
      val localMirror: FullMVTurnProxy = turn
      val mirrorProxy: FullMVTurnProxy = new SubsumableLockLocalCloneProxy(mirrorHost.lockHost, localMirror, reflectionHost.lockHost) with FullMVTurnProxy {
        override def addPredecessorAndReleasePhaseLock(predecessorSpanningTree: TransactionSpanningTreeNode[FullMVTurn]): Future[Unit] = {
          localMirror.addPredecessorAndReleasePhaseLock(predecessorSpanningTree.map(FullMVTurnLocalClone(_, mirrorHost)))
        }
        override def maybeNewReachableSubtree(attachBelow: FullMVTurn, spanningSubTreeRoot: TransactionSpanningTreeNode[FullMVTurn]): Future[Unit] = {
          localMirror.maybeNewReachableSubtree(FullMVTurnLocalClone(attachBelow, mirrorHost), spanningSubTreeRoot.map(FullMVTurnLocalClone(_, mirrorHost)))
        }
        override def acquirePhaseLockAndGetEstablishmentBundle(): Future[(TurnPhase.Type, TransactionSpanningTreeNode[FullMVTurn])] = {
          localMirror.acquirePhaseLockAndGetEstablishmentBundle().map { case (phase, spanningTree) =>
            (phase, spanningTree.map(FullMVTurnLocalClone(_, reflectionHost)))
          }(ReactiveTransmittable.notWorthToMoveToTaskpool)
        }
        override def asyncRemoteBranchComplete(forPhase: TurnPhase.Type): Unit = localMirror.asyncRemoteBranchComplete(forPhase)
        override def newSuccessor(successor: FullMVTurn): Future[Unit] = localMirror.newSuccessor(FullMVTurnLocalClone(successor, mirrorHost))
        override def asyncReleasePhaseLock(): Unit = localMirror.asyncReleasePhaseLock()
      }

      val reflection = new FullMVTurnReflection(reflectionHost, turn.guid, mirrorProxy)
      cacheNow(reflection)

      val reflectionProxy = new FullMVTurnReflectionProxy {
        override def newPhase(phase: TurnPhase.Type): Future[Unit] = reflection.newPhase(phase)
        override def newPredecessors(predecessors: Seq[Host.GUID]): Future[Unit] = reflection.newPredecessors(predecessors)
      }
      val (initPhase, initPreds) = turn.addReplicator(reflectionProxy)
      reflection.newPhase(initPhase)
      reflection.newPredecessors(initPreds)

      reflection
    }
  }
}
