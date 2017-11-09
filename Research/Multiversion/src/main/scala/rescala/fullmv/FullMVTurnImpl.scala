package rescala.fullmv

import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import java.util.concurrent.locks.{LockSupport, ReentrantLock}

import rescala.fullmv.TurnPhase.Type
import rescala.fullmv.mirrors.{FullMVTurnReflectionProxy, Host}
import rescala.fullmv.sgt.synchronization._

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{Await, Future}

class FullMVTurnImpl(override val host: FullMVEngine, override val guid: Host.GUID, val userlandThread: Thread, initialLock: SubsumableLock) extends FullMVTurn {
  // counts the sum of in-flight notifications, in-progress reevaluations.
  var activeBranches = new AtomicInteger(0)

  object replicatorLock
  val phaseLock = new ReentrantLock()
  object phaseParking
  @volatile var phase: TurnPhase.Type = TurnPhase.Initialized

  val subsumableLock: AtomicReference[SubsumableLock] = new AtomicReference(initialLock)
  val successorsIncludingSelf: ArrayBuffer[FullMVTurn] = ArrayBuffer(this) // this is implicitly a set
  val selfNode = new MutableTransactionSpanningTreeNode[FullMVTurn](this)
  @volatile var predecessorSpanningTreeNodes: Map[FullMVTurn, MutableTransactionSpanningTreeNode[FullMVTurn]] = Map(this -> selfNode)

  var replicators: Set[FullMVTurnReflectionProxy] = Set.empty

  override def asyncRemoteBranchComplete(forPhase: Type): Unit = {
    if(FullMVEngine.DEBUG) println(s"[${Thread.currentThread().getName}] $this branch on some remote completed")
    activeBranchDifferential(forPhase, -1)
  }

  def activeBranchDifferential(forState: TurnPhase.Type, differential: Int): Unit = {
    assert(phase == forState, s"$this received branch differential for wrong state ${TurnPhase.toString(forState)}")
    assert(differential != 0, s"$this received 0 branch diff")
    assert(activeBranches.get + differential >= 0, s"$this received branch diff into negative count")
    val remaining = activeBranches.addAndGet(differential)
    if(remaining == 0) {
      LockSupport.unpark(userlandThread)
    }
  }

  override def newBranchFromRemote(forPhase: Type): Unit = {
    assert(phase == forPhase, s"$this received branch differential for wrong state ${TurnPhase.toString(forPhase)}")
    if(FullMVEngine.DEBUG) println(s"[${Thread.currentThread().getName}] $this new branch on remote is actually loop-back to local")
    // technically, move one remote branch to a local branch, but as we don't count these separately, currently doing nothing.
  }

  override def addRemoteBranch(forPhase: TurnPhase.Type): Future[Unit] = {
    assert(phase == forPhase, s"$this received branch differential for wrong state ${TurnPhase.toString(forPhase)}")
    if(FullMVEngine.DEBUG) println(s"[${Thread.currentThread().getName}] $this new branch on some remote")
    activeBranches.getAndIncrement()
    Future.unit
  }

  //========================================================Local State Control============================================================

  def awaitAndSwitchPhase(newPhase: TurnPhase.Type): Unit = {
    assert(newPhase > this.phase, s"$this cannot progress backwards to phase $newPhase.")
    @tailrec def awaitAndAtomicCasPhaseAndGetReps(): Set[FullMVTurnReflectionProxy] = {
      awaitBranchCountZero()
      val compare = awaitAllPredecessorsPhase(newPhase)
      phaseLock.lock()
      val success = try {
        if (activeBranches.get == 0 && (predecessorSpanningTreeNodes eq compare)) {
          replicatorLock.synchronized {
            phaseParking.synchronized {
              this.phase = newPhase
              phaseParking.notifyAll()
            }
            if (newPhase == TurnPhase.Completed) {
              predecessorSpanningTreeNodes = Map.empty
              selfNode.children = Set.empty
              val l = subsumableLock.getAndSet(null)
              if (SubsumableLock.DEBUG) println(s"[${Thread.currentThread().getName}] $this deallocating, dropping reference on $l.")
              l.localSubRefs(1)
              host.dropInstance(guid, this)
            }
            if (FullMVEngine.DEBUG) println(s"[${Thread.currentThread().getName}] $this switched phase.")
            Some(replicators)
          }
        } else {
          None
        }
      } finally {
        phaseLock.unlock()
      }
      success match {
        case None => awaitAndAtomicCasPhaseAndGetReps()
        case Some(x) => x
      }
    }
    val reps = awaitAndAtomicCasPhaseAndGetReps()
    val forwards = reps.map(_.newPhase(newPhase))
    for(call <- forwards) {
      Await.result(call, host.timeout)
    }
  }


  private def awaitBranchCountZero(): Unit = {
    while (activeBranches.get > 0) {
      LockSupport.park(this)
    }
  }

  private def awaitAllPredecessorsPhase(atLeast: TurnPhase.Type) = {
    val preds = predecessorSpanningTreeNodes
    if (FullMVEngine.DEBUG) println(s"[${Thread.currentThread().getName}] $this awaiting phase $atLeast+ on predecessors ${preds.keySet - this}")
    preds.keySet.foreach { waitFor =>
      if(waitFor != this) waitFor.awaitPhase(atLeast)
    }
    preds
  }


  //========================================================Remote State Control============================================================

  override def awaitPhase(atLeast: TurnPhase.Type): Unit = phaseParking.synchronized {
    while(phase < atLeast) {
      phaseParking.wait()
    }
  }

  //========================================================Ordering Search and Establishment Interface============================================================

  def isTransitivePredecessor(txn: FullMVTurn): Boolean = {
    assert(txn.host == host, s"predecessor query for $txn before $this is hosted on ${txn.host} different from $host")
    predecessorSpanningTreeNodes.contains(txn)
  }


  override def acquirePhaseLockAndGetEstablishmentBundle(): Future[(TurnPhase.Type, TransactionSpanningTreeNode[FullMVTurn])] = {
    // TODO think about how and where to try{}finally{unlock()} this..
    phaseLock.lock()
    Future.successful((phase, selfNode))
  }

  def addPredecessorAndReleasePhaseLock(predecessorSpanningTree: TransactionSpanningTreeNode[FullMVTurn]): Future[Unit] = {
    @inline def predecessor = predecessorSpanningTree.txn
    assert(Await.result(predecessor.getLockedRoot, host.timeout).isDefined || predecessor.phase == TurnPhase.Completed, s"establishing order $predecessor -> $this: predecessor not locked")
    assert(Await.result(getLockedRoot, host.timeout).isDefined, s"establishing order $predecessor -> $this: successor not locked")
    assert(!isTransitivePredecessor(predecessor), s"attempted to establish already existing predecessor relation $predecessor -> $this")
    if(FullMVEngine.DEBUG) println(s"[${Thread.currentThread().getName}] $this new predecessor $predecessor.")
    val possiblyRemoteExecutions = successorsIncludingSelf.map(_.maybeNewReachableSubtree(this, predecessorSpanningTree))
    for(pre <- possiblyRemoteExecutions) {
      Await.result(pre, host.timeout)
    }
    phaseLock.unlock()
    Future.unit
  }

  override def maybeNewReachableSubtree(attachBelow: FullMVTurn, spanningSubTreeRoot: TransactionSpanningTreeNode[FullMVTurn]): Future[Unit] = {
    if (!isTransitivePredecessor(spanningSubTreeRoot.txn)) {
      val buffer = ArrayBuffer[FullMVTurn]()
      val reps = replicatorLock.synchronized {
        copySubTreeRootAndAssessChildren(attachBelow, spanningSubTreeRoot, buffer)
        replicators
      }

      val newSuccessorRemoteCalls = buffer.map(_.newSuccessor(this))
      val preds = buffer.map(_.guid)
      val newPredecessorRemoteCalls = reps.map(_.newPredecessors(preds))

      for(call <- newSuccessorRemoteCalls) {
        Await.result(call, host.timeout)
      }
      for(call <- newPredecessorRemoteCalls) {
        Await.result(call, host.timeout)
      }
    }
    Future.unit
  }

  private def copySubTreeRootAndAssessChildren(attachBelow: FullMVTurn, spanningSubTreeRoot: TransactionSpanningTreeNode[FullMVTurn], buffer: collection.generic.Growable[FullMVTurn]): Unit = {
    val newTransitivePredecessor = spanningSubTreeRoot.txn
    assert(newTransitivePredecessor.host == host, s"new predecessor $newTransitivePredecessor of $this is hosted on ${newTransitivePredecessor.host} different from $host")
    buffer += newTransitivePredecessor
    val copiedSpanningTreeNode = new MutableTransactionSpanningTreeNode(newTransitivePredecessor)
    predecessorSpanningTreeNodes += newTransitivePredecessor -> copiedSpanningTreeNode
    predecessorSpanningTreeNodes(attachBelow).children += copiedSpanningTreeNode

    for (child <- spanningSubTreeRoot.children) {
      if(!isTransitivePredecessor(child.txn)) {
        copySubTreeRootAndAssessChildren(newTransitivePredecessor, child, buffer)
      }
    }
  }

  override def newSuccessor(successor: FullMVTurn): Future[Unit] = {
    assert(successor.host == host, s"new successor $successor of $this is hosted on ${successor.host} different from $host")
    successorsIncludingSelf += successor
    Future.unit
  }

  override def asyncReleasePhaseLock(): Unit = phaseLock.unlock()

  //========================================================State Replication============================================================

  override def addReplicator(replicator: FullMVTurnReflectionProxy): (TurnPhase.Type, Seq[Host.GUID]) = {
    val (p, preds) = replicatorLock.synchronized {
      replicators += replicator
      (phase, predecessorSpanningTreeNodes.keySet)
    }
    (p, (preds - this).toSeq.map(_.guid))
  }

  //========================================================SSG SCC Mutual Exclusion Control============================================================

  override def getLockedRoot: Future[Option[Host.GUID]] = {
    val l = subsumableLock.get
    if(l == null) SubsumableLock.futureNone else l.getLockedRoot
  }
  override def lock(): Future[SubsumableLock] = {
    val l = subsumableLock.get()
    val res = l.lock0(1, lastHopWasGCd = false)
    res.map{ case (failedRefChanges, newRoot) =>
      casLockAndNotifyFailedRefChanges(l, failedRefChanges, newRoot)
      newRoot
    }(FullMVEngine.notWorthToMoveToTaskpool)
  }

  @tailrec final override def trySubsume(lockedNewParent: SubsumableLock): Future[TrySubsumeResult] = {
    val l = subsumableLock.get()
    if(l == null) {
      Future.successful(Deallocated)
    } else if(!l.tryNewLocalRef()) {
      assert(l != subsumableLock.get(), "lock was garbage collected despite still being in use?!")
      trySubsume(lockedNewParent)
    } else {
      if(SubsumableLock.DEBUG) println(s"[${Thread.currentThread().getName}] $this trySubsume acquired temporary ref on $l")
      val res = l.trySubsume0(0, lastHopWasGCd = false, lockedNewParent)
      res.map { case (failedRefChanges, newParentIfFailed) =>
        if(SubsumableLock.DEBUG) println(s"[${Thread.currentThread().getName}] $this trySubsume dropping temporary ref on $l")
        l.localSubRefs(1)
        casLockAndNotifyFailedRefChanges(l, failedRefChanges, newParentIfFailed.getOrElse(lockedNewParent))
        if(newParentIfFailed.isDefined) Blocked else Successful
      }(FullMVEngine.notWorthToMoveToTaskpool)
    }
  }

  private def casLockAndNotifyFailedRefChanges(from: SubsumableLock, failedRefChanges: Int, newRoot: SubsumableLock): Unit = {
    val finalFailedRefChanges = failedRefChanges + (if (from == newRoot) {
      0
    } else if(subsumableLock.compareAndSet(from, newRoot)) {
      if(SubsumableLock.DEBUG) println(s"[${Thread.currentThread().getName}] $this parent cas $from to $newRoot succeeded, dropping ref")
      from.localSubRefs(1)
      0
    } else {
      if(SubsumableLock.DEBUG) println(s"[${Thread.currentThread().getName}] $this parent cas $from to $newRoot failed due to contention")
      1
    })
    if(finalFailedRefChanges != 0) {
      if(SubsumableLock.DEBUG) println(s"[${Thread.currentThread().getName}] $this correcting $finalFailedRefChanges failed ref changes to $newRoot")
      newRoot.localSubRefs(finalFailedRefChanges)
    }
  }

  //========================================================ToString============================================================

  override def toString: String = s"FullMVTurn($guid on $host, ${TurnPhase.toString(phase)}${if(activeBranches.get != 0) s"(${activeBranches.get})" else ""})"
}
