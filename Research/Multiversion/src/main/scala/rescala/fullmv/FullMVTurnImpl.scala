package rescala.fullmv

import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import java.util.concurrent.locks.{LockSupport, ReentrantReadWriteLock}

import rescala.core.{InitialChange, ReSource}
import rescala.fullmv.mirrors.Host
import rescala.fullmv.sgt.synchronization._

import scala.annotation.elidable.ASSERTION
import scala.annotation.{elidable, tailrec}
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{Await, Future}

class FullMVTurnImpl(override val host: FullMVEngine, override val guid: Host.GUID, val userlandThread: Thread, initialLock: SubsumableLock) extends FullMVTurn {
  var initialChanges: collection.Map[ReSource[FullMVStruct], InitialChange[FullMVStruct]] = _

  // read and write order between the various volatiles:
  // phase is written after activeBranches and selfNode is read (phase switches only occur once no tasks remain or can be spawned by predecessors)
  // phase and predecessors/selfNode are written before replicators is read
  // whenever any combination of replicators, selfNode, predecessors, etc and phase is read, phase should thus be read last.
  // otherwise, phase may read as < Completed, but any of the other values may be nonsensical as they can have concurrently been updated by the Completed transition.

  // counts the sum of in-flight notifications, in-progress reevaluations.
  var activeBranches = new AtomicInteger(0) // write accesses are synchronized through atomic adds

  val phaseLock = new ReentrantReadWriteLock()
  @volatile var phase: TurnPhase.Type = TurnPhase.Uninitialized

  val subsumableLock: AtomicReference[SubsumableLock] = new AtomicReference(initialLock)
  val successorsIncludingSelf: ArrayBuffer[FullMVTurn] = ArrayBuffer(this) // implicit set, accesses are mutually exclusive through SGT SCC lock

  @volatile var selfNode = new MutableTransactionSpanningTreeNode[FullMVTurn](this) // implicit set, write accesses are mutually exclusive through SGT SCC lock
  @volatile var predecessorSpanningTreeNodes: Map[FullMVTurn, MutableTransactionSpanningTreeNode[FullMVTurn]] = Map(this -> selfNode) // write accesses are mutually exclusive through SGT SCC lock

  override def ensurePredecessorReplication(): Unit = Unit

  override def asyncRemoteBranchComplete(forPhase: TurnPhase.Type): Unit = {
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

  override def newBranchFromRemote(forPhase: TurnPhase.Type): Unit = {
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
    @inline @tailrec def awaitAndSwitchPhase0(firstUnknownPredecessorIndex: Int, parkAfter: Long, registeredForWaiting: FullMVTurn): Unit = {
      if (activeBranches.get() > 0) {
        if (registeredForWaiting != null) {
          registeredForWaiting.waiters.remove(this.userlandThread)
//          parkRestart.add(head.asInstanceOf[ReevaluationResultHandling[ReSource[FullMVStruct]]].node.toString)
//        } else if (parkAfter > 0) {
//          spinRestart.add(head.asInstanceOf[ReevaluationResultHandling[ReSource[FullMVStruct]]].node.toString)
        }
        awaitBranchCountZero()
        awaitAndSwitchPhase0(firstUnknownPredecessorIndex, 0L, null)
      } else if (firstUnknownPredecessorIndex == selfNode.size) {
        assert(registeredForWaiting == null, s"$this is still registered on $registeredForWaiting as waiter despite having finished waiting for it")
        phaseLock.writeLock.lock()
        // make thread-safe sure that we haven't received any new predecessors that might
        // not be in the next phase yet. Only once that's sure we can also thread-safe sure
        // check that no predecessors pushed any tasks into our queue anymore. And only then
        // can we phase switch.
        if (firstUnknownPredecessorIndex == selfNode.size && activeBranches.get() == 0) {
          this.phase = newPhase
          phaseLock.writeLock.unlock()
        } else {
          phaseLock.writeLock.unlock()
          awaitAndSwitchPhase0(firstUnknownPredecessorIndex, 0L, null)
        }
      } else {
        val currentUnknownPredecessor = selfNode.children(firstUnknownPredecessorIndex).txn
        if(currentUnknownPredecessor.phase < newPhase) {
          if (registeredForWaiting != null) {
            if (FullMVEngine.DEBUG) println(s"[${Thread.currentThread().getName}] $this parking for $currentUnknownPredecessor.")
            LockSupport.park(currentUnknownPredecessor)
            if (FullMVEngine.DEBUG) println(s"[${Thread.currentThread().getName}] $this unparked.")
            awaitAndSwitchPhase0(firstUnknownPredecessorIndex, 0L, currentUnknownPredecessor)
          } else {
            val now = System.nanoTime()
            val parkTimeSet = parkAfter > 0L
            if(parkTimeSet && now > parkAfter) {
              currentUnknownPredecessor.waiters.put(this.userlandThread, newPhase)
              awaitAndSwitchPhase0(firstUnknownPredecessorIndex, 0L, currentUnknownPredecessor)
            } else {
              val end = now + FullMVTurnImpl.CONSTANT_BACKOFF
              do {
                Thread.`yield`()
              } while (System.nanoTime() < end)
              awaitAndSwitchPhase0(firstUnknownPredecessorIndex, if(parkTimeSet) parkAfter else now + FullMVTurnImpl.MAX_BACKOFF, null)
            }
          }
        } else {
          if (registeredForWaiting != null) {
            currentUnknownPredecessor.waiters.remove(this.userlandThread)
            //            parkSwitch += 1
            //          } else if (parkAfter > 0) {
            //            spinSwitch += 1
          }
          awaitAndSwitchPhase0(firstUnknownPredecessorIndex + 1, 0L, null)
        }
      }
    }
    awaitAndSwitchPhase0(0, 0L, null)

    wakeWaitersAfterPhaseSwitch(newPhase)
    phaseReplicators.get.foreach(_.asyncNewPhase(newPhase))

    if (FullMVEngine.DEBUG) println(s"[${Thread.currentThread().getName}] $this switched phase.")
  }

  private def awaitBranchCountZero(): Unit = {
    while (activeBranches.get > 0) {
      LockSupport.park(this)
    }
  }

  private def beginPhase(phase: TurnPhase.Type): Unit = {
    assert(this.phase == TurnPhase.Uninitialized, s"$this already begun")
    assert(activeBranches.get() == 0, s"$this cannot begin $phase: ${activeBranches.get()} branches active!")
    assert(selfNode.size == 0, s"$this cannot begin $phase: already has predecessors!")
    this.phase = phase
    if (FullMVEngine.DEBUG) println(s"[${Thread.currentThread().getName}] $this begun.")
  }

  def beginFraming(): Unit = beginPhase(TurnPhase.Framing)
  def beginExecuting(): Unit = beginPhase(TurnPhase.Executing)

  //  def resetStatistics() = {
  //    spinSwitch = 0
  //    parkSwitch = 0
  //    spinRestart.clear()
  //    parkRestart.clear()
  //  }

  def completeFraming(): Unit = {
    assert(this.phase == TurnPhase.Framing, s"$this cannot complete framing: Not in framing phase")
    //    resetStatistics()
    awaitAndSwitchPhase(TurnPhase.Executing)
    assert(phase == TurnPhase.Executing, s"wat? $this")
    //    FullMVTurn.framesync.synchronized {
    //      val maybeCount1 = FullMVTurn.spinSwitchStatsFraming.get(spinSwitch)
    //      FullMVTurn.spinSwitchStatsFraming.put(spinSwitch, if(maybeCount1 == null) 1L else maybeCount1 + 1L)
    //      val maybeCount2 = FullMVTurn.parkSwitchStatsFraming.get(parkSwitch)
    //      FullMVTurn.parkSwitchStatsFraming.put(parkSwitch, if(maybeCount2 == null) 1L else maybeCount2 + 1L)
    //      val it1 = spinRestart.iterator()
    //      while(it1.hasNext) {
    //        val key = it1.next()
    //        val maybeCount3 = FullMVTurn.spinRestartStatsFraming.get(key)
    //        FullMVTurn.spinRestartStatsFraming.put(key, if (maybeCount3 == null) 1L else maybeCount3 + 1L)
    //      }
    //      val it2 = parkRestart.iterator()
    //      while(it2.hasNext) {
    //        val key = it2.next()
    //        val maybeCount4 = FullMVTurn.parkRestartStatsFraming.get(key)
    //        FullMVTurn.parkRestartStatsFraming.put(key, if (maybeCount4 == null) 1L else maybeCount4 + 1L)
    //      }
    //    }
  }

  def completeExecuting(): Unit = {
    assert(this.phase == TurnPhase.Executing, s"$this cannot complete executing: Not in executing phase")
    //    resetStatistics()
    awaitAndSwitchPhase(TurnPhase.Completed)
    phaseReplicators.set(null)
    predecessorReplicators.set(null)
    predecessorSpanningTreeNodes = Map.empty
    selfNode = null
    val l = subsumableLock.getAndSet(null)
    if (SubsumableLock.DEBUG) println(s"[${Thread.currentThread().getName}] $this deallocating, dropping reference on $l.")
    l.localSubRefs(1)
    host.dropInstance(guid, this)
    //    FullMVTurn.execsync.synchronized {
    //      val maybeCount1 = FullMVTurn.spinSwitchStatsExecuting.get(spinSwitch)
    //      FullMVTurn.spinSwitchStatsExecuting.put(spinSwitch, if(maybeCount1 == null) 1L else maybeCount1 + 1L)
    //      val maybeCount2 = FullMVTurn.parkSwitchStatsExecuting.get(parkSwitch)
    //      FullMVTurn.parkSwitchStatsExecuting.put(parkSwitch, if(maybeCount2 == null) 1L else maybeCount2 + 1L)
    //      val it1 = spinRestart.iterator()
    //      while(it1.hasNext) {
    //        val key = it1.next()
    //        val maybeCount3 = FullMVTurn.spinRestartStatsExecuting.get(key)
    //        FullMVTurn.spinRestartStatsExecuting.put(key, if (maybeCount3 == null) 1L else maybeCount3 + 1L)
    //      }
    //      val it2 = parkRestart.iterator()
    //      while(it2.hasNext) {
    //        val key = it2.next()
    //        val maybeCount4 = FullMVTurn.parkRestartStatsExecuting.get(key)
    //        FullMVTurn.parkRestartStatsExecuting.put(key, if (maybeCount4 == null) 1L else maybeCount4 + 1L)
    //      }
    //    }
  }

  //========================================================Ordering Search and Establishment Interface============================================================

  def isTransitivePredecessor(txn: FullMVTurn): Boolean = {
    assert(txn.host == host, s"predecessor query for $txn before $this is hosted on ${txn.host} different from $host")
    predecessorSpanningTreeNodes.contains(txn)
  }

  def acquirePhaseLockIfAtMost(maxPhase: TurnPhase.Type): Future[TurnPhase.Type] = Future.successful {
    val pOptimistic = phase
    if(pOptimistic > maxPhase) {
      pOptimistic
    } else {
      phaseLock.readLock().lock()
      val pSecure = phase
      if (pSecure > maxPhase) phaseLock.readLock().unlock()
      pSecure
    }
  }

  @elidable(ASSERTION) @inline
  def assertLockedState(predecessor: FullMVTurn): Unit = {
    assert(predecessor.phase > TurnPhase.Uninitialized, s"$this addition of initializing predecessor $predecessor should be impossible")
    val ownLock = getLockedRoot
    val otherLock = predecessor.getLockedRoot
    Await.result(ownLock, host.timeout) match {
      case LockedState(guid) =>
        Await.result(otherLock, host.timeout) match {
          case LockedState(otherGuid) => if(guid != otherGuid) throw new AssertionError(s"predecessor $predecessor and $this under different locks $otherGuid and $guid!")
          case UnlockedState => throw new AssertionError(s"predecessor $predecessor not locked!")
          case CompletedState => // ok
        }
      case UnlockedState => throw new AssertionError(s"$this not locked!")
      case CompletedState => throw new AssertionError(s"May no longer add predecessors to completed $this")
    }
  }

  def addPredecessor(predecessorSpanningTree: TransactionSpanningTreeNode[FullMVTurn]): Future[Boolean] = {
    val predecessor = predecessorSpanningTree.txn
    if(predecessor.phase == TurnPhase.Completed) {
      if (FullMVEngine.DEBUG) println(s"[${Thread.currentThread().getName}] $this aborting predecessor addition of known completed $predecessor")
      Future.successful(true)
    } else {
      assertLockedState(predecessor)
      assert(!isTransitivePredecessor(predecessor), s"attempted to establish already existing predecessor relation $predecessor -> $this")
      if (FullMVEngine.DEBUG) println(s"[${Thread.currentThread().getName}] $this adding predecessor $predecessor.")

      FullMVEngine.broadcast(successorsIncludingSelf)(_.maybeNewReachableSubtree(this, predecessorSpanningTree)).map(_ => predecessor.phase == TurnPhase.Completed)(FullMVEngine.notWorthToMoveToTaskpool)
    }
  }

  override def maybeNewReachableSubtree(attachBelow: FullMVTurn, spanningSubTreeRoot: TransactionSpanningTreeNode[FullMVTurn]): Future[Unit] = {
    if (!isTransitivePredecessor(spanningSubTreeRoot.txn)) {
      val incompleteCallsAccumulator = FullMVEngine.newAccumulator()

      // accumulate all changes offline and then batch-publish them in a single volatile write.
      // this prevents concurrent threads from seeing some of the newly established relations,
      // but not yet some transitive ones of those, which may violate several assertions
      // (although this doesn't actually break anything beyond these assertions)
      val (updatedTree, updatedAccu) = copySubTreeRootAndAssessChildren(predecessorSpanningTreeNodes, attachBelow, spanningSubTreeRoot, incompleteCallsAccumulator)
      predecessorSpanningTreeNodes = updatedTree

      val updated2Accu = FullMVEngine.accumulateBroadcastFutures(updatedAccu, predecessorReplicators.get) { _.newPredecessors(selfNode) }
      FullMVEngine.condenseCallResults(updated2Accu)
    } else {
      Future.unit
    }
  }

  private def copySubTreeRootAndAssessChildren(bufferPredecessorSpanningTreeNodes: Map[FullMVTurn, MutableTransactionSpanningTreeNode[FullMVTurn]], attachBelow: FullMVTurn, spanningSubTreeRoot: TransactionSpanningTreeNode[FullMVTurn], newSuccessorCallsAccumulator: FullMVEngine.CallAccumulator[Unit]): (Map[FullMVTurn, MutableTransactionSpanningTreeNode[FullMVTurn]], FullMVEngine.CallAccumulator[Unit]) = {
    val newTransitivePredecessor = spanningSubTreeRoot.txn
    assert(newTransitivePredecessor.host == host, s"new predecessor $newTransitivePredecessor of $this is hosted on ${newTransitivePredecessor.host} different from $host")
    if(spanningSubTreeRoot.txn.phase < TurnPhase.Completed) {
      val newSuccessorCall = newTransitivePredecessor.newSuccessor(this)
      var updatedAccu = FullMVEngine.accumulateFuture(newSuccessorCallsAccumulator, newSuccessorCall)

      val copiedSpanningTreeNode = new MutableTransactionSpanningTreeNode(newTransitivePredecessor)
      var updatedBufferPredecessorSpanningTreeNodes = bufferPredecessorSpanningTreeNodes + (newTransitivePredecessor -> copiedSpanningTreeNode)
      updatedBufferPredecessorSpanningTreeNodes(attachBelow).addChild(copiedSpanningTreeNode)

      val it = spanningSubTreeRoot.iterator()
      while (it.hasNext) {
        val child = it.next()
        if (!isTransitivePredecessor(child.txn)) {
          val (updated2Buffer, updated2Accu) = copySubTreeRootAndAssessChildren(updatedBufferPredecessorSpanningTreeNodes, newTransitivePredecessor, child, newSuccessorCallsAccumulator)
          updatedBufferPredecessorSpanningTreeNodes = updated2Buffer
          updatedAccu = updated2Accu
        }
      }
      (updatedBufferPredecessorSpanningTreeNodes, updatedAccu)
    } else {
      (bufferPredecessorSpanningTreeNodes, newSuccessorCallsAccumulator)
    }
  }

  override def newSuccessor(successor: FullMVTurn): Future[Unit] = {
    assert(successor.host == host, s"new successor $successor of $this is hosted on ${successor.host} different from $host")
    successorsIncludingSelf += successor
    Future.unit
  }

  override def asyncReleasePhaseLock(): Unit = phaseLock.readLock().unlock()

  //========================================================SSG SCC Mutual Exclusion Control============================================================

  override def getLockedRoot: Future[LockStateResult] = {
    val l = subsumableLock.get
    if(l == null) {
      assert(phase == TurnPhase.Completed, s"lock was deallocated although $this is still active?")
      CompletedState.futured
    } else {
      l.getLockedRoot.flatMap {
        case x@LockedState(lock) => Future.successful(x)
        case UnlockedState => UnlockedState.futured
        case ConcurrentDeallocation => getLockedRoot
      }(FullMVEngine.notWorthToMoveToTaskpool)
    }
  }
  override def tryLock(): Future[TryLockResult] = {
    if (SubsumableLock.DEBUG) println(s"[${Thread.currentThread().getName}] $this dispatching local tryLock request")
    tryLock0(0)
  }

  def tryLock0(hopCount: Int): Future[TryLockResult] = {
    val l = subsumableLock.get()
    if(l == null) {
      Deallocated.futured
    } else {
      l.tryLock0(hopCount).flatMap {
        case Locked0(failedRefChanges, newLockedRoot) =>
          val finalFailedRefChanges = failedRefChanges + trySwap(l, newLockedRoot)
          if(SubsumableLock.DEBUG) println(s"[${Thread.currentThread().getName}] $this tryLocked $newLockedRoot, correcting $finalFailedRefChanges failed ref changes (thread reference is retained and passed out)")
          if(finalFailedRefChanges > 0) newLockedRoot.localSubRefs(finalFailedRefChanges)
          Future.successful(Locked(newLockedRoot))
        case Blocked0(failedRefChanges, newRoot) =>
          val finalFailedRefChanges = 1 + failedRefChanges + trySwap(l, newRoot)
          if(SubsumableLock.DEBUG) println(s"[${Thread.currentThread().getName}] $this tryLock blocked under $newRoot, correcting $finalFailedRefChanges failed ref changes (includes thread reference)")
          newRoot.localSubRefs(finalFailedRefChanges)
          Blocked.futured
        case GarbageCollected0 =>
          assert(subsumableLock.get() != l, s"$l tryLock returned GC'd although it is still referenced")
          tryLock0(hopCount)
      }(FullMVEngine.notWorthToMoveToTaskpool)
    }
  }

  override def trySubsume(lockedNewParent: SubsumableLock): Future[TrySubsumeResult] = {
    if(SubsumableLock.DEBUG) println(s"[${Thread.currentThread().getName}] $this dispatching local trySubsume $lockedNewParent request")
    trySubsume0(0, lockedNewParent)
  }

  private def trySubsume0(hopCount: Int, lockedNewParent: SubsumableLock): Future[TrySubsumeResult] = {
    val l = subsumableLock.get()
    if(l == null) {
      Deallocated.futured
    } else {
      l.trySubsume0(hopCount, lockedNewParent).flatMap {
        case Successful0(failedRefChanges) =>
          val finalFailedRefChanges = failedRefChanges + trySwap(l, lockedNewParent)
          if(SubsumableLock.DEBUG) println(s"[${Thread.currentThread().getName}] $this trySubsumed under $lockedNewParent, correcting $finalFailedRefChanges failed ref changes")
          if(finalFailedRefChanges > 0) lockedNewParent.localSubRefs(finalFailedRefChanges)
          Successful.futured
        case Blocked0(failedRefChanges, newRoot) =>
          val finalFailedRefChanges = 1 + failedRefChanges + trySwap(l, newRoot)
          if(SubsumableLock.DEBUG) println(s"[${Thread.currentThread().getName}] $this trySubsume blocked under $newRoot, correcting $finalFailedRefChanges failed ref changes (includes thread reference)")
          newRoot.localSubRefs(finalFailedRefChanges)
          Blocked.futured
        case GarbageCollected0 =>
          assert(subsumableLock.get() != l, s"$l trySubsume returned GC'd although it is still referenced")
          trySubsume0(hopCount, lockedNewParent)
      }(FullMVEngine.notWorthToMoveToTaskpool)
    }
  }

  override def remoteTryLock(): Future[TryLockResult] = {
    if(SubsumableLock.DEBUG) println(s"[${Thread.currentThread().getName}] $this dispatching remote tryLock request")
    tryLock0(0).map{ res =>
      if(SubsumableLock.DEBUG) println(s"[${Thread.currentThread().getName}] $this returning tryLock result $res to remote (retaining thread reference as remote transfer reference)")
      res
    }(FullMVEngine.notWorthToMoveToTaskpool)
  }
  override def remoteTrySubsume(lockedNewParent: SubsumableLock): Future[TrySubsumeResult] = {
    if(SubsumableLock.DEBUG) println(s"[${Thread.currentThread().getName}] $this dispatching remote trySubsume $lockedNewParent request")
    trySubsume0(0, lockedNewParent).map { res =>
      if(SubsumableLock.DEBUG) println(s"[${Thread.currentThread().getName}] $this returning trySubsume $lockedNewParent request to remote, dropping remote parameter reference and retaining thread reference as remote transfer reference on result")
      lockedNewParent.localSubRefs(1)
      res
    }(FullMVEngine.notWorthToMoveToTaskpool)
  }

  private def trySwap(from: SubsumableLock, to: SubsumableLock): Int = {
    if (from == to) {
      0
    } else if(subsumableLock.compareAndSet(from, to)) {
      if(SubsumableLock.DEBUG) println(s"[${Thread.currentThread().getName}] $this parent cas $from to $to succeeded, dropping ref")
      from.localSubRefs(1)
      0
    } else {
      if(SubsumableLock.DEBUG) println(s"[${Thread.currentThread().getName}] $this parent cas $from to $to failed due to contention")
      1
    }
  }

  //========================================================ToString============================================================

  override def toString: String = s"FullMVTurn($guid on $host, ${TurnPhase.toString(phase)}${if(activeBranches.get != 0) s"(${activeBranches.get})" else ""})"
}

object FullMVTurnImpl {
  val CONSTANT_BACKOFF = 7500L // 7.5µs
  val MAX_BACKOFF = 100000L // 100µs

  //  object framesync
  //  var spinSwitchStatsFraming = new java.util.HashMap[Int, java.lang.Long]()
  //  var parkSwitchStatsFraming = new java.util.HashMap[Int, java.lang.Long]()
  //  val spinRestartStatsFraming = new java.util.HashMap[String, java.lang.Long]()
  //  val parkRestartStatsFraming =  new java.util.HashMap[String, java.lang.Long]()
  //  object execsync
  //  var spinSwitchStatsExecuting = new java.util.HashMap[Int, java.lang.Long]()
  //  var parkSwitchStatsExecuting = new java.util.HashMap[Int, java.lang.Long]()
  //  val spinRestartStatsExecuting = new java.util.HashMap[String, java.lang.Long]()
  //  val parkRestartStatsExecuting = new java.util.HashMap[String, java.lang.Long]()
}
