package rescala.fullmv

import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.LockSupport

import rescala.core.Initializer.InitValues
import rescala.fullmv.FramingBranchResult.FramingBranchEnd
import rescala.fullmv.NodeVersionHistory._

import scala.annotation.elidable.ASSERTION
import scala.annotation.{elidable, tailrec}
import scala.collection.mutable.ArrayBuffer

/**
  * A node version history datastructure
  * @param init the initial creating transaction
  * @param valuePersistency the value persistency descriptor
  * @tparam V the type of stored values
  * @tparam T the type of transactions
  * @tparam InDep the type of incoming dependency nodes
  * @tparam OutDep the type of outgoing dependency nodes
  */
abstract class NonblockingSkipListVersionHistory[V, T <: FullMVTurn, InDep, OutDep](init: T, val valuePersistency: InitValues[V]) {
  class WriteLogElement(val txn: T, val value: V, var pred: WriteLogElement)
  class QueuedVersion(val txn: T, @volatile var stableOrFinal: WriteLogElement, var pending: Int, var changed: Int, next: QueuedVersion, @volatile var sleeping: Boolean) extends AtomicReference[QueuedVersion](next) {
    def stabilize(stable: WriteLogElement): Unit = stableOrFinal = stable
    def stablized: WriteLogElement = {
      val stable = stableOrFinal
      assert(stable == null || txn.phase == TurnPhase.Executing, s"$this was set stable before executing?")
      if(stable.txn == txn) {
        assert(pending == 0 && changed == 0, s"$this was written but is still frame?")
        stable.pred
      } else {
        stable
      }
    }
    def finalized: WriteLogElement = if(pending == 0 && changed == 0) {
      val finalized = stableOrFinal
      assert(finalized == null || txn.phase == TurnPhase.Executing, s"$this was set stable/final before executing?")
      finalized
    } else {
      null
    }
  }

  // unsynchronized because written sequentially by notify/reevOut
  @volatile var log: WriteLogElement = new WriteLogElement(init, valuePersistency.initialValue, null)
  @volatile var latestValue: V
  // synchronized, written sequentially only if firstFrame.txn.phase == Executing && queueHead == firstFrame by notify/reevOut
  val lastRead = new AtomicReference[T](init)

  def enqueueFraming(txn: T, current: QueuedVersion): QueuedVersion
  def enqueueExecuting(txn: T, current: QueuedVersion): QueuedVersion
  def enqueueFollowFraming(txn: T, current: QueuedVersion): QueuedVersion
  // =================== STORAGE ====================

  // unsynchronized because written sequentially through object monitor
  @volatile var firstFrame: QueuedVersion = null
  // unsynchronized because written AND read sequentially by notify/reevOut
  var laggingGC: WriteLogElement = log

  // unsynchronized because written AND read sequentially by user computation
  var incomings: Set[InDep] = Set.empty
  // unsynchronized because written AND read sequentially through object monitor
  var outgoings: Set[OutDep] = Set.empty

  // =================== FRAMING ====================

  /**
    * entry point for regular framing
    *
    * @param txn the transaction visiting the node for framing
    */
  def incrementFrame(txn: T): FramingBranchResult[T, OutDep] = {
    var version: QueuedVersion = null
    while(version == null) version = enqueueFraming(txn, firstFrame)  // TODO handle before firstFrame case
    val result = synchronized { incrementFrame0(version) }
    result
  }

  /**
    * entry point for superseding framing
    * @param txn the transaction visiting the node for framing
    * @param supersede the transaction whose frame was superseded by the visiting transaction at the previous node
    */
  def incrementSupersedeFrame(txn: T, supersede: T): FramingBranchResult[T, OutDep] = {
    val version = enqueueFraming(txn, firstFrame)  // TODO handle failure case // TODO handle before firstFrame case
    val supersedeVersion = enqueueFraming(supersede, version)
    val result = synchronized {
      val res = incrementFrame0(version)
      supersedeVersion.pending -= 1
      res
    }
    result
  }

  private def incrementFrame0(version: QueuedVersion): FramingBranchResult[T, OutDep] = {
    // TODO maybe enough to synchronize if version.pending==0 before?
    version.pending += 1
    if(version == firstFrame) {
      assert(version.pending != 1, s"previously not a frame $version was already pointed to as firstFrame in $this")
      if(version.pending == 0) {
        // if first frame was removed (i.e., overtake compensation was resolved -- these cases mirror progressToNextWriteForNotification)
        firstFrame = firstFrame.get()
        while(firstFrame != null && firstFrame.pending == 0) {
          // keep moving further in the unlikely (?) case that the next version is also obsolete
          firstFrame = firstFrame.get()
        }
        if (firstFrame == null || firstFrame.pending < 0) {
          FramingBranchResult.FramingBranchEnd
        } else {
          FramingBranchResult.Frame(outgoings, firstFrame.txn)
        }
      } else {
        // just incremented an already existing and propagated frame
        FramingBranchResult.FramingBranchEnd
      }
    } else if(firstFrame == null || firstFrame.txn.isTransitivePredecessor(version.txn)) {
      // created a new frame
      assert(version.pending == 1, s"found overtake or frame compensation $version before firstFrame in $this")
      val oldFF = firstFrame
      firstFrame = version
      if(oldFF == null || oldFF.pending < 0) {
        FramingBranchResult.Frame(outgoings, version.txn)
      } else {
        FramingBranchResult.FrameSupersede(outgoings, version.txn, oldFF.txn)
      }
    } else {
      assert(version.txn.isTransitivePredecessor(firstFrame.txn), s"firstFrame $firstFrame apparently isn't ordered against incremented version $version")
      // created or incremented a non-first frame
      FramingBranchResult.FramingBranchEnd
    }
  }

  /*
   * =================== NOTIFICATIONS/ / REEVALUATION ====================
   */

  /**
    * entry point for change/nochange notification reception
    * @param txn the transaction sending the notification
    * @param changed whether or not the dependency changed
    */
  def notify(txn: T, changed: Boolean): NotificationResultAction[T, OutDep] = {
    var version: QueuedVersion = null
    while(version == null) version = enqueueExecuting(txn, firstFrame)
    val result = synchronized { notify0(version, changed) }
//    assertOptimizationsIntegrity(s"notify($txn, $changed) -> $result")
    result
  }

  /**
    * entry point for change/nochange notification reception with follow-up framing
    * @param txn the transaction sending the notification
    * @param changed whether or not the dependency changed
    * @param followFrame a transaction for which to create a subsequent frame, furthering its partial framing.
    */
  def notifyFollowFrame(txn: T, changed: Boolean, followFrame: T): NotificationResultAction[T, OutDep] = synchronized {
    var version: QueuedVersion = null
    while(version == null) version = enqueueExecuting(txn, firstFrame)
    var followFrame: QueuedVersion = null
    while(followFrame == null) followFrame = enqueueFollowFraming(txn, version)
    followFrame.pending += 1
    val result = synchronized { notify0(version, changed) }
//    assertOptimizationsIntegrity(s"notifyFollowFrame($txn, $changed, $followFrame) -> $result")
    result
  }

  private def notify0(version: QueuedVersion, changed: Boolean): NotificationResultAction[T, OutDep] = {
    // note: if the notification overtook a previous turn's notification with followFraming for this transaction,
    // pending may update from 0 to -1 here
    version.pending -= 1
    if (changed) {
      // note: if drop retrofitting overtook the change notification, change may update from -1 to 0 here!
      version.changed += 1
    }

    // check if the notification triggers subsequent actions
    if (version.pending == 0) {
      if (version == firstFrame) {
        if (version.changed > 0) {
          NotificationResultAction.GlitchFreeReady
        } else {
          // ResolvedFirstFrameToUnchanged
          findNextFrameForSuccessorOperation(/*version, version.lastWrittenPredecessorIfStable.get*/)
        }
      } else {
        if (version.changed > 0) {
          NotificationResultAction.GlitchFreeReadyButQueued
        } else {
          NotificationResultAction.ResolvedNonFirstFrameToUnchanged
        }
      }
    } else {
      NotificationResultAction.NotGlitchFreeReady
    }
  }

  def reevIn(txn: T): V = {
    assert(synchronized { firstFrame.txn == txn }, s"$txn called reevIn, but is not first frame owner in $this")
    latestValue
  }

  def reevOut(turn: T, maybeValue: Option[V]): NotificationResultAction.ReevOutResult[T, OutDep] = {
    val version = firstFrame
    assert(version.txn == turn, s"$turn called reevDone, but first frame is $version (different transaction)")
    assert(log.txn != turn, s"$turn cannot write twice: $version")

    val result = if(version.pending != 0) {
      NotificationResultAction.Glitched
    } else {
      assert(version.pending == 0, s"$this is not ready to be written")
      assert(version.changed > 0 || (version.changed == 0 && maybeValue.isEmpty), s"$turn cannot write changed=${maybeValue.isDefined} in $this")
      synchronized {
        if (maybeValue.isDefined) {
          log = new WriteLogElement(turn, maybeValue.get, log)
          if(laggingGC.txn.phase == TurnPhase.Completed) {
            laggingGC.pred = null
            laggingGC = log
          }
          latestValue = valuePersistency.unchange.unchange(maybeValue.get)
        }
        version.changed = 0
        findNextFrameForSuccessorOperation()
      }
    }
//    assertOptimizationsIntegrity(s"reevOut($turn, ${maybeValue.isDefined}) -> $result")
    result
  }

  private def findNextFrameForSuccessorOperation(/*finalizedVersion: Version, stabilizeTo: Version*/): NotificationResultAction.NotificationOutAndSuccessorOperation[T, OutDep] = {
    var newFirstFrame = firstFrame.get()
    while(newFirstFrame != null && newFirstFrame.pending == 0 && newFirstFrame.changed == 0){
      newFirstFrame = newFirstFrame.get()
    }
    firstFrame = newFirstFrame
    if(newFirstFrame == null) {
      NotificationResultAction.NotificationOutAndSuccessorOperation.NoSuccessor(outgoings)
    } else if (newFirstFrame.pending == 0) {
      assert(newFirstFrame.txn.phase == TurnPhase.Executing, s"change on non-executing $newFirstFrame")
      NotificationResultAction.NotificationOutAndSuccessorOperation.NextReevaluation(outgoings, newFirstFrame.txn)
    } else {
      NotificationResultAction.NotificationOutAndSuccessorOperation.FollowFraming(outgoings, newFirstFrame.txn)
    }
  }

  // =================== READ OPERATIONS ====================

  /**
    * entry point for before(this); may suspend.
    *
    * @param txn the executing transaction
    * @return the corresponding value from before this transaction, i.e., ignoring the transaction's
    *         own writes.
    */
  def dynamicBefore(txn: T): V = {
    val lr = lastRead.get()
    if(lr == txn || lr.isTransitivePredecessor(txn)) {
      staticBefore(txn)
    } else {
      val ff = firstFrame
      if (ff != null && ff.txn == txn) {
        staticBefore(txn)
      } else if (ff == null || (!txn.isTransitivePredecessor(ff.txn) && NonblockingSkipListVersionHistory.tryFixSuccessorOrder(txn, ff.txn))) {
        @tailrec @inline def retry(lr: T): V = {
          if (lr == txn || lr.isTransitivePredecessor(txn) || !NonblockingSkipListVersionHistory.tryFixPredecessorOrder(lr, txn) || lastRead.compareAndSet(lr, txn)) {
            staticBefore(txn)
          } else {
            retry(lastRead.get())
          }
        }
        retry(lr)
      } else {
        val versionInQueue = enqueueExecuting(txn, ff)
        if(versionInQueue == null) {
          dynamicBefore(txn)
        } else {
          assert(!versionInQueue.sleeping, s"someone else is sleeping on my version $versionInQueue?")
          versionInQueue.sleeping = true
          sleepForFF(versionInQueue)
          staticBefore(txn)
        }
      }
    }
  }

  @tailrec private def sleepForFF(version: QueuedVersion): Unit = {
    val ff = firstFrame
    if(ff != null && version.txn != ff.txn && version.txn.isTransitivePredecessor(ff.txn)) {
      LockSupport.park(NonblockingSkipListVersionHistory.this)
      sleepForFF(version)
    } else {
      version.sleeping = false
    }
  }

  def staticBefore(txn: T): V = {
    var predecessor = log
    while(predecessor.txn == txn || predecessor.txn.isTransitivePredecessor(txn)) {
      predecessor = predecessor.pred
    }
    assert(txn.isTransitivePredecessor(predecessor.txn) || predecessor.txn.phase == TurnPhase.Completed, s"staticBefore of $txn reading from non-predecessor $predecessor?")
    predecessor.value
  }

  /**
    * entry point for after(this); may suspend.
    * @param txn the executing transaction
    * @return the corresponding value from after this transaction, i.e., awaiting and returning the
    *         transaction's own write if one has occurred or will occur.
    */
  @tailrec final def dynamicAfter(txn: T): V = {
    val lr = lastRead.get()
    if(lr == txn || lr.isTransitivePredecessor(txn)) {
      staticAfter(txn)
    } else {
      val ff = firstFrame
      if (ff != null && ff.txn == txn) {
        staticAfter(txn)
      } else if (ff == null || (!txn.isTransitivePredecessor(ff.txn) && NonblockingSkipListVersionHistory.tryFixSuccessorOrder(txn, ff.txn))) {
         @tailrec @inline def retry(lr: T): V = {
          if (lr == txn || lr.isTransitivePredecessor(txn) || !NonblockingSkipListVersionHistory.tryFixPredecessorOrder(lr, txn) || lastRead.compareAndSet(lr, txn)) {
            staticAfter(txn)
          } else {
            retry(lastRead.get())
          }
        }
        retry(lr)
      } else {
        val versionInQueue = enqueueExecuting(txn, ff)
        if(versionInQueue == null) {
          dynamicAfter(txn)
        } else {
          assert(!versionInQueue.sleeping, s"someone else is sleeping on my version $versionInQueue?")
          versionInQueue.sleeping = true
          sleepForFF(versionInQueue)
          staticAfter(txn)
        }
      }
    }
  }

  @tailrec private def waitForStableThenReadBefore(prev: QueuedVersion, waiting: QueuedVersion): V = {
    assert(waiting.sleeping, s"someone forgot to set sleeping before sleeping!")
    val f = prev.finalized
    if (f != null) {
      val next = prev.get()
      if(next == waiting) {
        waiting.sleeping = false
        f.value
      } else {
        waitForStableThenReadBefore(next, waiting)
      }
    } else {
      LockSupport.park(NonblockingSkipListVersionHistory.this)
      waitForStableThenReadBefore(prev, waiting)
    }
  }

  def staticAfter(txn: T): V = {
    var ownOrPredecessor = log
    while(ownOrPredecessor.txn != txn && ownOrPredecessor.txn.isTransitivePredecessor(txn)) {
      ownOrPredecessor = ownOrPredecessor.pred
    }
    ownOrPredecessor.value
  }

  // =================== DYNAMIC OPERATIONS ====================

  def discover(txn: T, add: OutDep): (Seq[T], Option[T]) = synchronized {
    computeRetrofit(txn, synchronized {
      outgoings += add
      // TODO need to handle the case where txn lies beyond latestFinal, i.e., create trailing read version
      (log, firstFrame)
    })
  }

  def drop(txn: T, remove: OutDep): (Seq[T], Option[T]) = synchronized {
    computeRetrofit(txn, synchronized {
      outgoings -= remove
      // TODO need to handle the case where txn lies beyond latestFinal, i.e., create trailing read version
      (log, firstFrame)
    })
  }

  private def computeRetrofit(txn: T, latestFinalAndFirstFrame: (WriteLogElement, QueuedVersion)): (Seq[T], Option[T]) = {
    var maybeFirstFrame = latestFinalAndFirstFrame._2
    val frame = if(maybeFirstFrame != null) {
      assert(maybeFirstFrame.txn.isTransitivePredecessor(txn), s"can only retrofit into the past, but $txn is in the future of firstframe $firstFrame!")
      Some(maybeFirstFrame.txn)
    } else {
      None
    }

    var logLine = latestFinalAndFirstFrame._1
    var retrofits: List[T] = Nil
    while(logLine.txn == txn || logLine.txn.isTransitivePredecessor(txn) || (
      logLine.txn.phase != TurnPhase.Completed &&
      !txn.isTransitivePredecessor(logLine.txn) &&
      !NonblockingSkipListVersionHistory.tryRecordRelationship(logLine.txn, txn, logLine.txn, txn)
    )) {
      retrofits ::= logLine.txn
      logLine = logLine.pred
    }
    (retrofits, frame)
  }

  /**
    * performs the reframings on the sink of a discover(n, this) with arity +1, or drop(n, this) with arity -1
    * @param successorWrittenVersions the reframings to perform for successor written versions
    * @param maybeSuccessorFrame maybe a reframing to perform for the first successor frame
    * @param arity +1 for discover adding frames, -1 for drop removing frames.
    */
  def retrofitSinkFrames(successorWrittenVersions: Seq[T], maybeSuccessorFrame: Option[T], arity: Int): Unit = synchronized {
    require(math.abs(arity) == 1)
    var current = firstFrame
    for(txn <- successorWrittenVersions) {
      val current = enqueueExecuting(txn, current)
      // note: if drop retrofitting overtook a change notification, changed may update from 0 to -1 here!
      current.changed += arity
    }

    if (maybeSuccessorFrame.isDefined) {
      val txn = maybeSuccessorFrame.get
      val version = enqueueFollowFraming(txn, current)
      // note: conversely, if a (no)change notification overtook discovery retrofitting, pending may change
      // from -1 to 0 here. No handling is required for this case, because firstFrame < position is an active
      // reevaluation (the one that's executing the discovery) and will afterwards progressToNextWrite, thereby
      // executing this then-ready reevaluation, but for now the version is guaranteed not stable yet.
      version.pending += arity
    }
    // cannot make this assertion here because dynamic events might make the firstFrame not a frame when dropping the only incoming changed dependency..
    //assertOptimizationsIntegrity(s"retrofitSinkFrames(writes=$successorWrittenVersions, maybeFrame=$maybeSuccessorFrame)")
  }
}

trait DedicatedSearchStrategies[V, T <: FullMVTurn, InDep, OutDep] extends NonblockingSkipListVersionHistory[V, T, InDep, OutDep] {
  @tailrec override final def enqueueFraming(txn: T, current: QueuedVersion): QueuedVersion = {
    val next = current.get
    if (next != null && next.txn == txn) {
      next
    } else if (next == null || next.txn.isTransitivePredecessor(txn)) {
      if(NonblockingSkipListVersionHistory.tryFixPredecessorOrder(current.txn, txn)) {
        val waiting = new QueuedVersion(txn, null, 0, 0, next, false)
        if (current.compareAndSet(next, waiting)) {
          waiting
        } else {
          enqueueFraming(txn, current)
        }
      } else {
        null
      }
    } else {
      enqueueFraming(txn, next)
    }
  }

  @tailrec override final def enqueueExecuting(txn: T, current: QueuedVersion): QueuedVersion = {
    val next = current.get
    if (next != null && next.txn == txn) {
      next
    } else if (next == null || NonblockingSkipListVersionHistory.tryFixSuccessorOrder(txn, next.txn)) {
      if(NonblockingSkipListVersionHistory.tryFixPredecessorOrder(current.txn, txn)) {
        val waiting = new QueuedVersion(txn, null, 0, 0, next, false)
        if (current.compareAndSet(next, waiting)) {
          waiting
        } else {
          enqueueExecuting(txn, current)
        }
      } else {
        null
      }
    } else {
      enqueueExecuting(txn, next)
    }
  }

  override final def enqueueFollowFraming(txn: T, current: QueuedVersion): QueuedVersion = {
    if(txn.phase == TurnPhase.Executing) {
      enqueueExecuting(txn, current)
    } else {
      enqueueFollowFraming(txn, current)
    }
  }
  @tailrec final def enqueueFollowFramingFraming(txn: T, current: QueuedVersion): QueuedVersion = {
    val next = current.get()
    if (next != null && next.txn == txn) {
      next
    } else if (next == null || next.txn.isTransitivePredecessor(txn) /* assuming we are still Framing, always go as far back as possible */ ) {
      if (txn.isTransitivePredecessor(current.txn) || (current.txn.phase match {
        case TurnPhase.Completed => true
        case TurnPhase.Executing => NonblockingSkipListVersionHistory.tryRecordRelationship(current.txn, txn, current.txn, txn)
        case TurnPhase.Framing => /* and only if still Framing becomes relevant, ensure that we still are */
          txn.acquirePhaseLockIfAtMost(TurnPhase.Framing) <= TurnPhase.Framing && (try {
            NonblockingSkipListVersionHistory.tryRecordRelationship(current.txn, txn, current.txn, txn)
          } finally {
            txn.asyncReleasePhaseLock()
          })
      })) {
        val q = new QueuedVersion(txn, null, 0, 0, next, false)
        if (current.compareAndSet(next, q)) {
          q
        } else {
          enqueueFollowFramingFraming(txn, current)
        }
      } else {
        null
      }
    } else {
      enqueueFollowFramingFraming(txn, next)
    }
  }
}

trait SingleGenericSearchAlgorithm[V, T <: FullMVTurn, InDep, OutDep] extends NonblockingSkipListVersionHistory[V, T, InDep, OutDep] {
  override def enqueueFraming(txn: T, current: QueuedVersion): QueuedVersion = enqueue(txn, current)
  override def enqueueExecuting(txn: T, current: QueuedVersion): QueuedVersion = enqueue(txn, current)
  override def enqueueFollowFraming(txn: T, current: QueuedVersion): QueuedVersion = enqueue(txn, current)

  @tailrec final def enqueue(txn: T, current: QueuedVersion): QueuedVersion = {
    val next = current.get()
    if(next != null && next.txn == txn) {
      next
    } else if(next == null || next.txn.isTransitivePredecessor(txn) || (txn.phase == TurnPhase.Executing && {
      val res = next.txn.acquirePhaseLockIfAtMost(TurnPhase.Framing) <= TurnPhase.Framing
      if (res) try {
        val established = NonblockingSkipListVersionHistory.tryRecordRelationship(txn, next.txn, next.txn, txn)
        assert(established, s"failed to order executing $txn before locked-framing ${next.txn}?")
      } finally {
        next.txn.asyncReleasePhaseLock()
      }
      res
    })) {
      if(txn.isTransitivePredecessor(current.txn) || (current.txn.phase match {
        case TurnPhase.Completed => true
        case TurnPhase.Executing => NonblockingSkipListVersionHistory.tryRecordRelationship(current.txn, txn, current.txn, txn)
        case TurnPhase.Framing =>
          txn.acquirePhaseLockIfAtMost(TurnPhase.Framing) <= TurnPhase.Framing && (try {
            NonblockingSkipListVersionHistory.tryRecordRelationship(current.txn, txn, current.txn, txn)
          } finally {
            txn.asyncReleasePhaseLock()
          })
      })) {
        val q = new QueuedVersion(txn, null, 0, 0, next, false)
        if(current.compareAndSet(next, q)) {
          q
        } else {
          enqueue(txn, current)
        }
      } else {
        null
      }
    } else {
      enqueue(txn, next)
    }
  }
}

object NonblockingSkipListVersionHistory {
  /**
    * @param attemptPredecessor intended predecessor transaction
    * @param succToRecord intended successor transactoin
    * @param defender encountered transaction
    * @param contender processing (and thus phase-locked) transaction
    * @return success if relation was established, false if reverse relation was established concurrently
    */
  def tryRecordRelationship(attemptPredecessor: FullMVTurn, succToRecord: FullMVTurn, defender: FullMVTurn, contender: FullMVTurn): Boolean = {
    SerializationGraphTracking.tryLock(defender, contender, UnlockedUnknown) match {
      case x: LockedSameSCC =>
        try {
          if (succToRecord.isTransitivePredecessor(attemptPredecessor)) {
            // relation already recorded
            true
          } else if (attemptPredecessor.isTransitivePredecessor(succToRecord)) {
            // reverse relation already recorded
            false
          } else {
            val tree = attemptPredecessor.selfNode
            if (tree == null) {
              assert(attemptPredecessor.phase == TurnPhase.Completed, s"$attemptPredecessor selfNode was null but isn't completed?")
              // relation no longer needs recording because predecessor completed concurrently
              true
            } else {
              succToRecord.addPredecessor(tree)
              // relation newly recorded
              true
            }
          }
        } finally {
          x.unlock()
        }
      case otherwise =>
        Thread.`yield`()
        if (attemptPredecessor.phase == TurnPhase.Completed) {
          // relation no longer needs recording because predecessor completed concurrently
          true
        } else if(succToRecord.isTransitivePredecessor(attemptPredecessor)) {
          // relation already recorded
          true
        } else if (attemptPredecessor.isTransitivePredecessor(succToRecord)) {
          // reverse relation already recorded
          false
        } else {
          // retry
          tryRecordRelationship(attemptPredecessor, succToRecord, defender, contender)
        }
    }
  }

  /**
    * @param txn must be executing
    * @param succ is attempted to be phase-locked in framing
    * @return success if phase-locking succ to framing worked, failure if succ started executing
    */
  def tryFixSuccessorOrder(txn: FullMVTurn, succ: FullMVTurn): Boolean = {
    succ.isTransitivePredecessor(txn) || {
      val res = succ.acquirePhaseLockIfAtMost(TurnPhase.Framing) < TurnPhase.Executing
      if (res) try {
        val established = tryRecordRelationship(txn, succ, succ, txn)
        assert(established, s"failed to order executing $txn before locked-framing $succ?")
      } finally {
        succ.asyncReleasePhaseLock()
      }
      res
    }
  }

  /**
    * @param pred may be framing or executing
    * @param txn must be phase-locked to framing if pred can be framing
    * @return success if order was established, failure if reverse order was established concurrently
    */
  def tryFixPredecessorOrder(pred: FullMVTurn, txn: FullMVTurn): Boolean = {
    txn.isTransitivePredecessor(pred) || pred.phase == TurnPhase.Completed || tryRecordRelationship(pred, txn, pred, txn)
  }
}
