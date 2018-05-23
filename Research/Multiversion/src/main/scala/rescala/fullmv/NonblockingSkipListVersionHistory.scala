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
class NonblockingSkipListVersionHistory[V, T <: FullMVTurn, InDep, OutDep](init: T, val valuePersistency: InitValues[V]) {
  class Version(val txn: T, val next: AtomicReference[Version], val lastWrittenPredecessorIfStable: AtomicReference[Version], @volatile var stable: Boolean, var pending: Int, var changed: Int, @volatile var value: Option[V]) /*extends MyManagedBlocker*/ {
    def read(): V = {
      assert(stable, "reading unstable "+this)
      assert(value.isDefined, "reading un-written "+this)
      value.get
    }

    @volatile var stableWaiters: Int = 0

    // less common blocking case
    // fake lazy val without synchronization, because it is accessed only while the node's monitor is being held.
    def blockForStable(): Unit = {
      if (!stable) {
        stableWaiters += 1
        assert(Thread.currentThread() == txn.userlandThread, s"this assertion is only valid without a threadpool .. otherwise it should be txn==txn, but that would require txn to be spliced in here which is annoying while using the managedblocker interface")
        while (!stable) {
          if (FullMVEngine.DEBUG) println(s"[${Thread.currentThread().getName}] parking for stable ${Version.this}")
          LockSupport.park(this)
          if (FullMVEngine.DEBUG) println(s"[${Thread.currentThread().getName}] unparked on ${Version.this}")
        }
        stableWaiters -= 1
      }
    }

    override def toString: String = {
      val nxt = next.get()
      s"Version($txn, next=${if(nxt == null) "<End>" else nxt.txn}, prev=${lastWrittenPredecessorIfStable.get().txn}, stable=$stable, pending=$pending, changed=$changed, value=$value"
//      if(isWritten){
//        s"Written($txn, v=${value.get})"
//      } else if (isReadOrDynamic) {
//        (if(isStable) "Stable" else "Unstable") + s"Marker($txn)"
//      } else if (isOvertakeCompensation) {
//        s"OvertakeCompensation($txn, ${if (isStable) "stable" else "unstable"}, pending=$pending, changed=$changed)"
//      } else if(isFrame) {
//        if(isStable) {
//          if(isReadyForReevaluation) {
//            s"Active($txn)"
//          } else {
//            s"FirstFrame($txn, pending=$pending, changed=$changed)"
//          }
//        } else {
//          if(isReadyForReevaluation) {
//            s"Queued($txn)"
//          } else {
//            s"Frame($txn, pending=$pending, changed=$changed)"
//          }
//        }
//      } else {
//        "UnknownVersionCase!(" + txn + ", " + (if(isStable) "stable" else "unstable") + ", pending=" + pending + ", changed=" + changed + ", value = " + value + ")"
//      }
    }
  }

  // =================== STORAGE ====================

  val latestFinal = new AtomicReference(new Version(init, new AtomicReference(null), lastWrittenPredecessorIfStable = null, stable = true, pending = 0, changed = 0, Some(valuePersistency.initialValue)))
  var firstFrame = null.asInstanceOf[Version]
  val laggingTail = new AtomicReference(latestFinal.get)
  var laggingGC = latestFinal.get

  var latestValue: V = latestFinal.get.read
  var incomings: Set[InDep] = Set.empty
  var outgoings: Set[OutDep] = Set.empty

  sealed trait FindMaxResult
  case class Found(version: Version) extends FindMaxResult
  case class NotFound(orderedBefore: Version, orderedAfter: Version) extends FindMaxResult

  // =================== FRAMING SEARCH AND INSERT ===================== //

  @tailrec private def findMaxUpToFramingBackwards(txn: T, knownMax: Version): FindMaxResult = {
    val pred = if(knownMax == null) laggingTail.get else knownMax.lastWrittenPredecessorIfStable.get
    val predTxn = pred.txn
    if(predTxn == txn) {
      Found(pred)
    } if (predTxn.isTransitivePredecessor(txn)) {
      findMaxUpToFramingBackwards(txn, pred)
    } else {
      findMaxUpToFramingForwards(txn, pred, knownMax)
    }
  }

  @tailrec private def findMaxUpToFramingForwards(txn: T, tryMin: Version, knownMax: Version): FindMaxResult = {
    val succ = tryMin.next.get
    if(succ.txn == txn) {
      Found(succ)
    } else if (succ == knownMax || succ.txn.isTransitivePredecessor(txn)) {
      val tryMinTxn = tryMin.txn
      if(tryMinTxn.phase == TurnPhase.Completed || txn.isTransitivePredecessor(tryMinTxn) || tryRecordRelationship(tryMinTxn, txn, tryMinTxn, txn)) {
        NotFound(tryMin, knownMax)
      } else {
        findMaxUpToFramingForwards(txn, tryMin.lastWrittenPredecessorIfStable.get, tryMin)
      }
    } else {
      findMaxUpToFramingForwards(txn, succ, knownMax)
    }
  }

  @tailrec private def ensureVersionFraming(txn: T, knownMax: Version = null): Version = {
    findMaxUpToFramingBackwards(txn, knownMax) match {
      case Found(v) => v
      case NotFound(pred, succ) =>
        val v = new Version(txn, new AtomicReference(succ), new AtomicReference(pred), stable = false, pending = 0, changed = 0, value = None)
        if(pred.next.compareAndSet(succ, v)) {
          if(succ == null) {
            laggingTail.compareAndSet(pred, succ) // Failure is ok
          } else {
            succ.lastWrittenPredecessorIfStable.compareAndSet(pred, v) // Failure is ok
          }
          v
        } else {
          ensureVersionFraming(txn, succ)
        }
    }
  }

  // =================== GENEARL SEARCH AND INSERT ===================== //

  /**
    * @param attemptPredecessor
    * @param succToRecord
    * @param defender
    * @param contender
    * @return true relation is final (recorded by self or concurrent thread, or predecessor completed), false if reverse relation was recorded concurrently
    */
  private def tryRecordRelationship(attemptPredecessor: T, succToRecord: T, defender: T, contender: T): Boolean = {
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

  // =================== NOTIFYING SEARCH AND INSERT ===================== //

  @tailrec private def findMaxUpToNotifyingForward(txn: T, current: Version): FindMaxResult = {
    val next = current.next.get
    if(next.txn == txn) {
      Found(next)
    } else if (next == null || next.txn.isTransitivePredecessor(txn)) {
      if(txn.isTransitivePredecessor(current.txn) || tryRecordRelationship(current.txn, txn, current.txn, txn)) {
        NotFound(current, next)
      } else {
        // reverse relation was recorded, so fall back a link
        findMaxUpToNotifyingForward(txn, current.lastWrittenPredecessorIfStable.get())
      }
    } else next.txn.phase match {
      case TurnPhase.Completed =>
        findMaxUpToNotifyingForward(txn, next)
      case TurnPhase.Executing =>
        findMaxUpToNotifyingForward(txn, next)
      case TurnPhase.Framing =>
        next.txn.acquirePhaseLockIfAtMost(TurnPhase.Framing) match {
          case TurnPhase.Completed =>
            findMaxUpToNotifyingForward(txn, next)
          case TurnPhase.Executing =>
            findMaxUpToNotifyingForward(txn, next)
          case TurnPhase.Framing =>
            try {
              // order successor
              if (!next.txn.isTransitivePredecessor(txn)) {
                val recorded = tryRecordRelationship(txn, next.txn, next.txn, txn)
                assert(recorded, s"tryRecord should be impossible to fail here because ${next.txn} is phase-locked to a lower phase than $txn")
              }
            } finally {
              next.txn.asyncReleasePhaseLock()
            }
            // try order predecessor
            if(txn.isTransitivePredecessor(current.txn) || tryRecordRelationship(current.txn, txn, current.txn, txn)) {
              NotFound(current, next)
            } else {
              // reverse relation was recorded, so fall back a link
              findMaxUpToNotifyingForward(txn, current.lastWrittenPredecessorIfStable.get())
            }
          case otherwise =>
            if (otherwise <= TurnPhase.Framing) next.txn.asyncReleasePhaseLock()
            throw new AssertionError(s"phase-locking ${next.txn} returned unhandled phase $otherwise")
        }
      case otherwise => throw new AssertionError(s"unexpected phase $otherwise in version search for $txn from $next")
    }
  }

  /**
    * @return the latest finalized version's next pointer
    */
  @tailrec private def helpFinalize(): Version = {
    val current = latestFinal.get()
    val lastWrittenPredecessor = if(current.value.isDefined) current else current.lastWrittenPredecessorIfStable.get
    val finalized = finalizeNext(current, lastWrittenPredecessor)
    if (latestFinal.compareAndSet(current, finalized)) {
      finalized.next.get()
    } else {
      // maybe could short-cut to another cas instead of re-iterating the list:
      // if(stabilize < newStable || (stabilize.isCompleted && newStable) then done.
      // if(newStable < stabilize || (newStable.isCompleted && !stabilize.isCompleted))
      // if(newStable.isCompleted && stabilize.isCompleted) then restart helpStable().
      // then tryCas(newStable, stabilize)
      helpFinalize()
    }
  }
  @tailrec private def finalizeNext(current: Version, lastWrittenPredecessor: Version): Version = {
    val stabilize = current.next.get()
    if(stabilize == null || stabilize.txn.phase == TurnPhase.Framing) {
      current
    } else {
      assert(stabilize.txn.phase >= TurnPhase.Executing, s"unexpedted phase on ${stabilize.txn}")
      if(!stabilize.stable) {
        stabilize.lastWrittenPredecessorIfStable.set(lastWrittenPredecessor)
        stabilize.stable = true
        if (stabilize.stableWaiters > 0) LockSupport.unpark(stabilize.txn.userlandThread)
      }
      if(stabilize.pending == 0 && stabilize.changed == 0) {
        // if(stabilize.finalWaiters > 0) LockSupport.unpark(stabilize.txn.userlandThread)
        finalizeNext(stabilize, lastWrittenPredecessor)
      } else {
        current
      }
    }
  }

  @tailrec private def ensureVersionNotifying(txn: T, from: Version = helpFinalize()): Version = {
    findMaxUpToNotifyingForward(txn, from) match {
      case Found(v) => v
      case NotFound(pred, succ) =>
        val v = new Version(txn, new AtomicReference(succ), new AtomicReference(pred), stable = false, pending = 0, changed = 0, value = None)
        if(pred.next.compareAndSet(succ, v)) {
          v
        } else {
          ensureVersionNotifying(txn, succ)
        }
    }
  }

  @tailrec private def findMaxUpToFollowFramingBackwards(txn: T, knownMin: Version, knownMax: Version): FindMaxResult = {
    val pred = if(knownMax == null) laggingTail.get else knownMax.lastWrittenPredecessorIfStable.get
    val predTxn = pred.txn
    if(predTxn == txn) {
      Found(pred)
    } if (predTxn.isTransitivePredecessor(txn)) {
      findMaxUpToFollowFramingBackwards(txn, knownMin, pred)
    } else {
      findMaxUpToFollowFramingForwards(txn, pred, knownMin, knownMax)
    }
  }

  @tailrec private def findMaxUpToFollowFramingForwards(txn: T, tryMin: Version, knownMin: Version, knownMax: Version): FindMaxResult = {
    val succ = tryMin.next.get
    if(succ.txn == txn) {
      Found(succ)
    } else if(succ == knownMax || succ.txn.isTransitivePredecessor(txn)) {
      val tryMinTxn = tryMin.txn
      if(tryMinTxn.phase == TurnPhase.Completed || txn.isTransitivePredecessor(tryMinTxn)) {
        NotFound(tryMin, knownMax)
      } else if (txn.acquirePhaseLockIfAtMost(TurnPhase.Framing) > TurnPhase.Framing) {
          findMaxUpToNotifyingForward(txn, knownMin)
      } else {
        val orderEstablished = try {
          tryRecordRelationship(tryMinTxn, txn, tryMinTxn, txn)
        } finally {
          txn.asyncReleasePhaseLock()
        }
        if (orderEstablished) {
          NotFound(tryMin, knownMax)
        } else {
          findMaxUpToFollowFramingForwards(txn, tryMin.lastWrittenPredecessorIfStable.get, knownMin, tryMin)
        }
      }
    } else {
      findMaxUpToFollowFramingForwards(txn, succ, knownMin, knownMax)
    }
  }

  @tailrec private def ensureVersionFollowFraming(txn: T, from: Version, knownMax: Version = null): Version = {
    if(txn.phase == TurnPhase.Framing) {
      findMaxUpToFollowFramingBackwards(txn, from, knownMax) match {
        case Found(v) => v
        case NotFound(pred, succ) =>
          val v = new Version(txn, new AtomicReference(succ), new AtomicReference(pred), stable = false, pending = 0, changed = 0, value = None)
          if (pred.next.compareAndSet(succ, v)) {
            v
          } else {
            ensureVersionFollowFraming(txn, pred, succ)
          }
      }
    } else {
      ensureVersionNotifying(txn, from)
    }
  }

  // =================== FRAMING ====================

  /**
    * entry point for regular framing
    *
    * @param txn the transaction visiting the node for framing
    */
  def incrementFrame(txn: T): FramingBranchResult[T, OutDep] = {
    val version = ensureVersionFraming(txn)
    val result = synchronized { incrementFrame0(version) }
    result
  }

  /**
    * entry point for superseding framing
    * @param txn the transaction visiting the node for framing
    * @param supersede the transaction whose frame was superseded by the visiting transaction at the previous node
    */
  def incrementSupersedeFrame(txn: T, supersede: T): FramingBranchResult[T, OutDep] = {
    val supersedeVersion = ensureVersionFraming(supersede)
    val version = ensureVersionFraming(txn, supersedeVersion)
    val result = synchronized {
      supersedeVersion.pending -= 1 // TODO move this behind synchronized? check clean-ups in incrementFrame0
      incrementFrame0(version)
    }
    result
  }

  private def incrementFrame0(version: Version): FramingBranchResult[T, OutDep] = {
    version.pending += 1
    if(version == firstFrame) {
      assert(version.pending != 1, s"previously not a frame $version was already pointed to as firstFrame in $this")
      if(version.pending == 0) {
        // if first frame was removed (i.e., overtake compensation was resolved -- these cases mirror progressToNextWriteForNotification)
        firstFrame = firstFrame.next.get()
        while(firstFrame != null && firstFrame.pending == 0) {
          // keep moving further in the unlikely (?) case that the next version is also obsolete
          firstFrame = firstFrame.next.get()
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
    val version = ensureVersionNotifying(txn)
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
    val version = ensureVersionNotifying(txn)
    val followFrame = ensureVersionFollowFraming(txn, version)
    followFrame.pending += 1
    val result = synchronized { notify0(version, changed) }
//    assertOptimizationsIntegrity(s"notifyFollowFrame($txn, $changed, $followFrame) -> $result")
    result
  }

  private def notify0(version: Version, changed: Boolean): NotificationResultAction[T, OutDep] = {
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
    assert(version.value.isEmpty, s"$turn cannot write twice: $version")

    val result = if(version.pending != 0) {
      NotificationResultAction.Glitched
    } else {
      assert(version.pending == 0, s"$this is not ready to be written")
      assert(version.changed > 0 || (version.changed == 0 && maybeValue.isEmpty), s"$turn cannot write changed=${maybeValue.isDefined} in $this")
      synchronized {
        if (maybeValue.isDefined) {
          latestValue = valuePersistency.unchange.unchange(maybeValue.get)
          version.value = maybeValue
        }
        version.changed = 0
        findNextFrameForSuccessorOperation()
      }
    }
//    assertOptimizationsIntegrity(s"reevOut($turn, ${maybeValue.isDefined}) -> $result")
    result
  }

  private def findNextFrameForSuccessorOperation(/*finalizedVersion: Version, stabilizeTo: Version*/): NotificationResultAction.NotificationOutAndSuccessorOperation[T, OutDep] = {
    var nextFrame = helpFinalize()
    while(nextFrame != null && nextFrame.pending == 0 && nextFrame.changed == 0) {
      nextFrame = nextFrame.next.get()
    }
    firstFrame = nextFrame
    if(nextFrame == null) {
      NotificationResultAction.NotificationOutAndSuccessorOperation.NoSuccessor(outgoings)
    } else if (nextFrame.pending == 0) {
      assert(nextFrame.changed > 0, s"stopped at non-frame $nextFrame")
      assert(nextFrame.txn.phase == TurnPhase.Executing, s"change on non-executing $nextFrame")
      NotificationResultAction.NotificationOutAndSuccessorOperation.NextReevaluation(outgoings, nextFrame.txn)
    } else {
      NotificationResultAction.NotificationOutAndSuccessorOperation.FollowFraming(outgoings, nextFrame.txn)
    }
  }

  // =================== READ OPERATIONS ====================

  /**
    * ensures at least a read version is stored to track executed reads or dynamic operations.
    * @param txn the executing transaction
    * @return the version's position.
    */
  private def ensureReadVersion(txn: T, knownOrderedMinPos: Int = latestGChint + 1): Int = {
    assert(knownOrderedMinPos > latestGChint, s"nonsensical minpos $knownOrderedMinPos <= latestGChint $latestGChint")
    if(knownOrderedMinPos == size) {
      assert(txn.isTransitivePredecessor(_versions(knownOrderedMinPos - 1).txn) || _versions(knownOrderedMinPos - 1).txn.phase == TurnPhase.Completed, s"illegal $knownOrderedMinPos: predecessor ${_versions(knownOrderedMinPos - 1).txn} not ordered in $this")
      arrangeVersionArrayAndCreateVersion(size, txn)
    } else if (_versions(latestKnownStable).txn == txn) {
      lastGCcount = 0
      latestKnownStable
    } else {
      val (insertOrFound, _) = findOrPigeonHolePropagatingPredictive(txn, knownOrderedMinPos, fromFinalPredecessorRelationIsRecorded = true, size, toFinalRelationIsRecorded = true, UnlockedUnknown)
      if(insertOrFound < 0) {
        arrangeVersionArrayAndCreateVersion(-insertOrFound, txn)
      } else {
        lastGCcount = 0
        insertOrFound
      }
    }
  }

  /**
    * entry point for before(this); may suspend.
    *
    * @param txn the executing transaction
    * @return the corresponding [[Version.value]] from before this transaction, i.e., ignoring the transaction's
    *         own writes.
    */
  def dynamicBefore(txn: T): V = {
    //    assert(!valuePersistency.isTransient, s"$txn invoked dynamicBefore on transient node")
    val version = synchronized {
      val pos = ensureReadVersion(txn)
      // DO NOT INLINE THIS! it breaks the code! see https://scastie.scala-lang.org/briJDRO3RCmIMEd1zApmBQ
      _versions(pos)
    }
    if(!version.isStable) version.blockForStable()
    version.lastWrittenPredecessorIfStable.value.get
  }

  def staticBefore(txn: T): V = {
    var predecessor = latestFinal.get()
    while(predecessor.txn == txn || predecessor.txn.isTransitivePredecessor(txn)) {
      predecessor = predecessor.lastWrittenPredecessorIfStable.get()
    }
    predecessor.read()
  }

  /**
    * entry point for after(this); may suspend.
    * @param txn the executing transaction
    * @return the corresponding [[Version.value]] from after this transaction, i.e., awaiting and returning the
    *         transaction's own write if one has occurred or will occur.
    */
  def dynamicAfter(txn: T): V = {
    val version = synchronized {
      val pos = ensureReadVersion(txn)
      // DO NOT INLINE THIS! it breaks the code! see https://scastie.scala-lang.org/briJDRO3RCmIMEd1zApmBQ
      _versions(pos)
    }
    if(!version.isStable) version.blockForStable()
    if (version.value.isDefined) {
      version.value.get
    } else {
      valuePersistency.unchange.unchange(version.lastWrittenPredecessorIfStable.value.get)
    }
  }

  def staticAfter(txn: T): V = {
    var ownOrPredecessor = latestFinal.get()
    while(ownOrPredecessor.txn != txn && ownOrPredecessor.txn.isTransitivePredecessor(txn)) {
      ownOrPredecessor = ownOrPredecessor.lastWrittenPredecessorIfStable.get()
    }
    ownOrPredecessor.read()
  }

  // =================== DYNAMIC OPERATIONS ====================

  /**
    * entry point for discover(this, add). May suspend.
    * @param txn the executing reevaluation's transaction
    * @param add the new edge's sink node
    * @return the appropriate [[Version.value]].
    */
  def discover(txn: T, add: OutDep): (Seq[T], Option[T]) = synchronized {
    computeRetrofit(txn, synchronized {
      outgoings += add
      // TODO need to handle the case where txn lies beyond latestFinal, i.e., create trailing read version
      (latestFinal.get(), firstFrame)
    })
  }

  /**
    * entry point for drop(this, ticket.issuer); may suspend temporarily.
    * @param txn the executing reevaluation's transaction
    * @param remove the removed edge's sink node
    */
  def drop(txn: T, remove: OutDep): (Seq[T], Option[T]) = synchronized {
    computeRetrofit(txn, synchronized {
      outgoings -= remove
      // TODO need to handle the case where txn lies beyond latestFinal, i.e., create trailing read version
      (latestFinal.get(), firstFrame)
    })
  }

  private def computeRetrofit(txn: T, latestFinalAndFirstFrame: (Version, Version)): (Seq[T], Option[T]) = {
    var maybeFirstFrame = latestFinalAndFirstFrame._2
    val frame = if(maybeFirstFrame != null) {
      assert(maybeFirstFrame.txn.isTransitivePredecessor(txn), s"can only retrofit into the past, but $txn is in the future of firstframe $firstFrame!")
      Some(maybeFirstFrame.txn)
    } else {
      None
    }

    var assess = latestFinalAndFirstFrame._1
    if(assess.value.isEmpty) assess = assess.lastWrittenPredecessorIfStable.get()
    var retrofits: List[T] = Nil
    while(assess.txn.isTransitivePredecessor(txn) || (
      assess.txn.phase != TurnPhase.Completed &&
      !txn.isTransitivePredecessor(assess.txn) &&
      !tryRecordRelationship(assess.txn, txn, assess.txn, txn)
    )) {
      retrofits ::= assess.txn
      assess = assess.lastWrittenPredecessorIfStable.get
    }
  }

  /**
    * performs the reframings on the sink of a discover(n, this) with arity +1, or drop(n, this) with arity -1
    * @param successorWrittenVersions the reframings to perform for successor written versions
    * @param maybeSuccessorFrame maybe a reframing to perform for the first successor frame
    * @param arity +1 for discover adding frames, -1 for drop removing frames.
    */
  def retrofitSinkFrames(successorWrittenVersions: Seq[T], maybeSuccessorFrame: Option[T], arity: Int): Unit = synchronized {
    require(math.abs(arity) == 1)
    var minPos = firstFrame
    for(txn <- successorWrittenVersions) {
      val position = ensureReadVersion(txn, minPos)
      val version = _versions(position)
      // note: if drop retrofitting overtook a change notification, changed may update from 0 to -1 here!
      version.changed += arity
      minPos = position + 1
    }

    if (maybeSuccessorFrame.isDefined) {
      val txn = maybeSuccessorFrame.get
      val position = ensureReadVersion(txn, minPos)
      val version = _versions(position)
      // note: conversely, if a (no)change notification overtook discovery retrofitting, pending may change
      // from -1 to 0 here. No handling is required for this case, because firstFrame < position is an active
      // reevaluation (the one that's executing the discovery) and will afterwards progressToNextWrite, thereby
      // executing this then-ready reevaluation, but for now the version is guaranteed not stable yet.
      version.pending += arity
    }
    // cannot make this assertion here because dynamic events might make the firstFrame not a frame when dropping the only incoming changed dependency..
    //assertOptimizationsIntegrity(s"retrofitSinkFrames(writes=$successorWrittenVersions, maybeFrame=$maybeSuccessorFrame)")
  }

  /**
    * rewrites all affected [[Version.out]] of the source this during drop(this, delta) with arity -1 or
    * discover(this, delta) with arity +1, and collects transactions for retrofitting frames on the sink node
    * @param position the executing transaction's version's position
    * @param delta the outgoing dependency to add/remove
    * @param arity +1 to add, -1 to remove delta to each [[Version.out]]
    * @return a list of transactions with written successor versions and maybe the transaction of the first successor
    *         frame if it exists, for which reframings have to be performed at the sink.
    */
  private def retrofitSourceOuts(position: Int, delta: OutDep, arity: Int): (Seq[T], Option[T]) = {
    require(math.abs(arity) == 1)
    // allocate array to the maximum number of written versions that might follow
    // (any version at index firstFrame or later can only be a frame, not written)
    val sizePrediction = math.max(firstFrame - position, 0)
    val successorWrittenVersions = new ArrayBuffer[T](sizePrediction)
    var maybeSuccessorFrame: Option[T] = None
    for(pos <- position until size) {
      val version = _versions(pos)
      if(arity < 0) version.out -= delta else version.out += delta
      // as per above, this is implied false if pos >= firstFrame:
      if(maybeSuccessorFrame.isEmpty) {
        if(version.isWritten){
          successorWrittenVersions += version.txn
        } else if (version.isFrame) {
          maybeSuccessorFrame = Some(version.txn)
        }
      }
    }
    if(successorWrittenVersions.size > sizePrediction) System.err.println(s"FullMV retrofitSourceOuts predicted size max($firstFrame - $position, 0) = $sizePrediction, but size eventually was ${successorWrittenVersions.size}")
    assertOptimizationsIntegrity(s"retrofitSourceOuts(from=$position, outdiff=$arity $delta) -> (writes=$successorWrittenVersions, maybeFrame=$maybeSuccessorFrame)")
    (successorWrittenVersions, maybeSuccessorFrame)
  }
}
