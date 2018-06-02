package rescala.fullmv

import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.LockSupport

import rescala.core.Initializer.InitValues
import rescala.core.Pulse

import scala.annotation.tailrec

sealed trait MaybeWritten[+V]
case object Unwritten extends MaybeWritten[Nothing]
case class Written[V](valueForSelf: V, valueForFuture: V) extends MaybeWritten[V]

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
  class QueuedVersion(val txn: T, @volatile var previousWriteIfStable: QueuedVersion, var pending: Int, var changed: Int, @volatile var value: MaybeWritten[V], next: QueuedVersion, @volatile var sleeping: Boolean) extends AtomicReference[QueuedVersion](next) {
    def stabilize(write: QueuedVersion): Unit = {
      assert(write.value != Unwritten, s"may not stabilize to unwritten $write")
      previousWriteIfStable = write
    }

    def readForSelf: V = value match {
      case Unwritten => throw new NoSuchElementException(this + " is not written!")
      case Written(valueForSelf, _) => valueForSelf
    }
    def readForFuture: V = value match {
      case Unwritten => throw new NoSuchElementException(this + " is not written!")
      case Written(_, valueForFuture) => valueForFuture
    }

    override def toString: String = {
      s"Version($hashCode of $txn, p=$pending, c=$changed, v=$value, stable=$previousWriteIfStable)"
    }
  }

  // unsynchronized because written sequentially by notify/reevOut
  val laggingLatestStable = new AtomicReference(new QueuedVersion(init, null, 0,0, {
    val iv = valuePersistency.initialValue
    Written(iv, valuePersistency.unchange.unchange(iv))
  }, null, false))
  @volatile var latestValue: V = laggingLatestStable.get.readForFuture
  // synchronized, written sequentially only if firstFrame.txn.phase == Executing && queueHead == firstFrame by notify/reevOut

  // =================== STORAGE ====================

  // unsynchronized because written sequentially through object monitor
  @volatile var firstFrame: QueuedVersion = null
  // unsynchronized because written AND read sequentially by notify/reevOut
  var lazyPeriodicGC: QueuedVersion = laggingLatestStable.get

  // unsynchronized because written AND read sequentially by user computation
  var incomings: Set[InDep] = Set.empty
  // unsynchronized because written AND read sequentially through object monitor
  var outgoings: Set[OutDep] = Set.empty

  def isReachable(from: QueuedVersion, to: QueuedVersion): Boolean = {
    from == to || (from != null && isReachable(from.get(), to))
  }

  // more efficient version but unable to include assertions:
//  private def setLatestStable(to: QueuedVersion): Unit = {
//    assert(to.value != Unwritten || to.previousWriteIfStable != null, s"new latestStable not stable: $to")
//    val from = laggingLatestStable.get()
//    assert(isReachable(from, to), s"new latest stable $to is not reachable from $from")
//    if(from != to) {
//      laggingLatestStable.set(to)
//      from.lazySet(from)
//    }
//  }
  // less efficient implementation but safe to include assertions:
  @tailrec private def setLatestStable(to: QueuedVersion): Unit = {
    assert(to.value != Unwritten || to.previousWriteIfStable != null, s"new latestStable not stable: $to")
    val from = laggingLatestStable.get()
    if(from != to) {
      if(laggingLatestStable.compareAndSet(from, to)) {
        assert(isReachable(from, to), s"new latest stable $to is not reachable from $from")
        from.lazySet(from)
      } else {
        setLatestStable(to)
      }
    }
  }

  protected def casLatestStable(from: QueuedVersion, to: QueuedVersion): Unit = {
    assert(to.value != Unwritten || to.previousWriteIfStable != null, s"new latestStable not stable: $to")
    if (from != to && laggingLatestStable.compareAndSet(from, to)){
      assert(isReachable(from, to), s"new latest stable $to is not reachable from $from")
      from.lazySet(from)
    }
  }

  @tailrec private def enqueueFraming(txn: T, current: QueuedVersion): QueuedVersion = {
    val next = current.get
    if (next == current) {
      if(NonblockingSkipListVersionHistory.DEBUG) println(s"[${Thread.currentThread().getName}] enqueueFraming fell of the list on $next.")
      null
    } else if (next != null && next.txn == txn) {
      if(NonblockingSkipListVersionHistory.DEBUG) println(s"[${Thread.currentThread().getName}] enqueueFraming found target $next.")
      next
    } else if (next == null || next.txn.isTransitivePredecessor(txn)) {
      if(NonblockingSkipListVersionHistory.tryFixPredecessorOrderIfNotFixedYet(current.txn, txn)) {
        val v = tryInsertVersion(txn, current, next, null)
        if(v != null){
          if(NonblockingSkipListVersionHistory.DEBUG) println(s"[${Thread.currentThread().getName}] enqueueFraming created $v between $current and $next.")
          v
        } else {
          enqueueFraming(txn, current)
        }
      } else {
        if(NonblockingSkipListVersionHistory.DEBUG) println(s"[${Thread.currentThread().getName}] enqueueFraming failed speculative ordering $current < $txn.")
        null
      }
    } else {
      enqueueFraming(txn, next)
    }
  }

  @tailrec private def enqueueExecuting(txn: T, current: QueuedVersion): QueuedVersion = {
    val next = current.get
    if(next == current) {
      if(NonblockingSkipListVersionHistory.DEBUG) println(s"[${Thread.currentThread().getName}] enqueueExecuting fell of the list on $next.")
      null
    } else if (next != null && next.txn == txn) {
      if(NonblockingSkipListVersionHistory.DEBUG) println(s"[${Thread.currentThread().getName}] enqueueExecuting found target $next.")
      next
    } else if (next == null || NonblockingSkipListVersionHistory.tryFixSuccessorOrderIfNotFixedYet(txn, next.txn)) {
      if(NonblockingSkipListVersionHistory.tryFixPredecessorOrderIfNotFixedYet(current.txn, txn)) {
        val v = tryInsertVersion(txn, current, next, null)
        if(v != null){
          if(NonblockingSkipListVersionHistory.DEBUG) println(s"[${Thread.currentThread().getName}] enqueueExecuting created $v between $current and $next.")
          v
        } else {
          enqueueExecuting(txn, current)
        }
      } else {
        if(NonblockingSkipListVersionHistory.DEBUG) println(s"[${Thread.currentThread().getName}] enqueueExecuting failed speculative ordering $current < $txn.")
        null
      }
    } else {
      enqueueExecuting(txn, next)
    }
  }

  private def enqueueFollowFraming(txn: T, current: QueuedVersion): QueuedVersion = {
    if(txn.phase == TurnPhase.Executing) {
      if(NonblockingSkipListVersionHistory.DEBUG) println(s"[${Thread.currentThread().getName}] enqueueFollowFraming delegating to enqueueNotifying for executing $txn.")
      enqueueExecuting(txn, current)
    } else {
      enqueueFollowFramingFraming(txn, current)
    }
  }
  @tailrec private def enqueueFollowFramingFraming(txn: T, current: QueuedVersion): QueuedVersion = {
    val next = current.get()
    assert(next != current, s"notifying found $next fell off the list, which should be impossible because notify happens behind firstFrame, but only before firstFrame can fall of the list")
    if (next != null && next.txn == txn) {
      if(NonblockingSkipListVersionHistory.DEBUG) println(s"[${Thread.currentThread().getName}] enqueueFollowFraming found target $next.")
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
        val v = tryInsertVersion(txn, current, next, null)
        if(v != null){
          if(NonblockingSkipListVersionHistory.DEBUG) println(s"[${Thread.currentThread().getName}] enqueueFollowFraming created $v between $current and $next.")
          v
        } else {
          enqueueFollowFramingFraming(txn, current)
        }
      } else {
        if(NonblockingSkipListVersionHistory.DEBUG) println(s"[${Thread.currentThread().getName}] enqueueFollowFraming failed speculative ordering $current < $txn.")
        null
      }
    } else {
      enqueueFollowFramingFraming(txn, next)
    }
  }

  // =================== FRAMING ====================

  /**
    * entry point for regular framing
    *
    * @param txn the transaction visiting the node for framing
    */
  def incrementFrame(txn: T): FramingBranchResult[T, OutDep] = {
    var version: QueuedVersion = null
    while(version == null) version = enqueueFraming(txn, laggingLatestStable.get)
    val result = synchronized { incrementFrame0(version) }
    if(NonblockingSkipListVersionHistory.DEBUG) println(s"[${Thread.currentThread().getName}] incrementFrame $txn => $result.")
    result
  }

  /**
    * entry point for superseding framing
    * @param txn the transaction visiting the node for framing
    * @param supersede the transaction whose frame was superseded by the visiting transaction at the previous node
    */
  def incrementSupersedeFrame(txn: T, supersede: T): FramingBranchResult[T, OutDep] = {
    var version: QueuedVersion = null
    while(version == null) version = enqueueFraming(txn, laggingLatestStable.get)
    val supersedeVersion = enqueueFraming(supersede, version)
    val result = synchronized {
      supersedeVersion.pending -= 1
      incrementFrame0(version)
    }
    if(NonblockingSkipListVersionHistory.DEBUG) println(s"[${Thread.currentThread().getName}] incrementFrame $txn, $supersede => $result.")
    result
  }

  private def incrementFrame0(version: QueuedVersion): FramingBranchResult[T, OutDep] = {
    // TODO maybe enough to synchronize if version.pending==0 before?
    version.pending += 1
    var ff = firstFrame
    if(version == ff) {
      assert(version.pending != 1, s"previously not a frame $version was already pointed to as firstFrame in $this")
      if(version.pending == 0) {
        // if first frame was removed (i.e., overtake compensation was resolved -- these cases mirror progressToNextWriteForNotification)
        ff = ff.get()
        while(ff!= null && ff.pending == 0) {
          // keep moving further in the unlikely (?) case that the next version is also obsolete
          ff = ff.get()
        }
        firstFrame = ff
        if (ff == null || ff.pending < 0) {
          FramingBranchResult.FramingBranchEnd
        } else {
          FramingBranchResult.Frame(outgoings, ff.txn)
        }
      } else {
        // just incremented an already existing and propagated frame
        FramingBranchResult.FramingBranchEnd
      }
    } else if(ff == null || ff.txn.isTransitivePredecessor(version.txn)) {
      // created a new frame
      assert(version.pending == 1, s"found overtake or frame compensation $version before firstFrame in $this")
      firstFrame = version
      if(ff == null || ff.pending < 0) {
        FramingBranchResult.Frame(outgoings, version.txn)
      } else {
        FramingBranchResult.FrameSupersede(outgoings, version.txn, ff.txn)
      }
    } else {
      assert(version.txn.isTransitivePredecessor(ff.txn), s"firstFrame $ff apparently isn't ordered against incremented version $version")
      // created or incremented a non-first frame
      FramingBranchResult.FramingBranchEnd
    }
  }

  /*
   * =================== NOTIFICATIONS/ / REEVALUATION ====================
   */

  private def enqueueNotifying(txn: T): QueuedVersion = {
    var version: QueuedVersion = null
    while (version == null) {
      val ff = firstFrame
      version = if (ff.txn == txn) {
        if (NonblockingSkipListVersionHistory.DEBUG) println(s"[${Thread.currentThread().getName}] notify on firstFrame $ff.")
        // less performant code with assertions
        @tailrec def ensureLatestStable(): Unit = {
          val ls = laggingLatestStable.get
          if (ls != ff) {
            if (ff.previousWriteIfStable == null) {
              ff.stabilize(if (ls.value != Unwritten) ls else ls.previousWriteIfStable)
              if (NonblockingSkipListVersionHistory.DEBUG) println(s"[${Thread.currentThread().getName}] notify stabilized sneaker $ff.")
            }
            if(laggingLatestStable.compareAndSet(ls, ff)) {
              assert(isReachable(ls, ff), s"firstFrame = new latest stable = $ff is not reachable from $ls")
              ls.lazySet(ls)
            } else {
              ensureLatestStable()
            }
          }
        }
        ensureLatestStable()
        // more performant code incompatible with assertions
//        val ls = laggingLatestStable.get
//        if (ls != ff) {
//          if (ff.previousWriteIfStable == null) {
//            ff.previousWriteIfStable = if (ls.value != Unwritten) ls else ls.previousWriteIfStable
//            if (NonblockingSkipListVersionHistory.DEBUG) println(s"[${Thread.currentThread().getName}] notify stabilized sneaker $ff.")
//          }
//          laggingLatestStable.set(ff)
//          ls.lazySet(ls)
//        }
        ff
      } else {
        enqueueExecuting(txn, ff)
      }
    }
    version
  }

  /**
    * entry point for change/nochange notification reception
    * @param txn the transaction sending the notification
    * @param changed whether or not the dependency changed
    */
  def notify(txn: T, changed: Boolean): NotificationResultAction[T, OutDep] = {
    val version = enqueueNotifying(txn)
    val result = synchronized { notify0(version, changed) }
    if(NonblockingSkipListVersionHistory.DEBUG) println(s"[${Thread.currentThread().getName}] notify $txn with change=$changed => $result.")
    result
  }

  /**
    * entry point for change/nochange notification reception with follow-up framing
    * @param txn the transaction sending the notification
    * @param changed whether or not the dependency changed
    * @param followFrame a transaction for which to create a subsequent frame, furthering its partial framing.
    */
  def notifyFollowFrame(txn: T, changed: Boolean, followFrame: T): NotificationResultAction[T, OutDep] = synchronized {
    val version: QueuedVersion = enqueueNotifying(txn)
    var followVersion: QueuedVersion = null
    while(followVersion == null) followVersion = enqueueFollowFraming(followFrame, version)
    val result = synchronized {
      followVersion.pending += 1
      notify0(version, changed)
    }
    if(NonblockingSkipListVersionHistory.DEBUG) println(s"[${Thread.currentThread().getName}] notify $txn with change=$changed and followFrame $followFrame => $result.")
    result
  }

  private def notify0(version: QueuedVersion, changed: Boolean): NotificationResultAction[T, OutDep] = {
    if (changed) {
      // note: if drop retrofitting overtook the change notification, change may update from -1 to 0 here!
      version.changed += 1
    }
    // note: if the notification overtook a previous turn's notification with followFraming for this transaction,
    // or drop retrofitting with firstFrame retrofit for followFraming, then pending may update from 0 to -1 here
    version.pending -= 1

    // check if the notification triggers subsequent actions
    if (version.pending == 0) {
      if (version == firstFrame) {
        if (version.changed > 0) {
          NotificationResultAction.GlitchFreeReady
        } else {
          // ResolvedFirstFrameToUnchanged
          findNextFrameForSuccessorOperation(version, version.previousWriteIfStable)
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
    assert(firstFrame.txn == txn, s"$txn called reevIn, but is not first frame owner in $this")
    latestValue
  }

  def reevOut(turn: T, maybeValue: Option[V]): NotificationResultAction.ReevOutResult[T, OutDep] = {
    val version = firstFrame
    assert(version.txn == turn, s"$turn called reevDone, but first frame is $version (different transaction)")
    assert(version.pending > 0 || (version.pending == 0 && (version.changed > 0 || (version.changed == 0 && maybeValue.isEmpty))), s"$turn cannot write $maybeValue to $version")
    assert(version.value == Unwritten, s"$turn cannot write twice: $version")

    val result = if(version.pending != 0) {
      if(NonblockingSkipListVersionHistory.TRACE_VALUES) println(s"[${Thread.currentThread().getName}] $turn reevaluation of $version was glitched and will be repeated.")
      NotificationResultAction.Glitched
    } else {
      assert(version.pending == 0, s"$this is not ready to be written")
      assert(version.changed > 0 || (version.changed == 0 && maybeValue.isEmpty), s"$turn cannot write changed=${maybeValue.isDefined} in $this")
      if(NonblockingSkipListVersionHistory.DEBUG || FullMVEngine.DEBUG) maybeValue match {
        case Some(Pulse.Exceptional(t)) =>
          System.err.println(s"[${Thread.currentThread().getName}] WARNING: Glich-free reevaluation result is exceptional:")
          maybeValue.get.asInstanceOf[Pulse.Exceptional].throwable.printStackTrace()
        case _ => // ignore
      }
      synchronized {
        val stabilizeTo = if (maybeValue.isDefined) {
          version.value = Written(maybeValue.get, valuePersistency.unchange.unchange(maybeValue.get))
          if(lazyPeriodicGC.txn.phase == TurnPhase.Completed) {
            lazyPeriodicGC.previousWriteIfStable = null
            lazyPeriodicGC = version
          }
          latestValue = version.readForFuture
          version
        } else {
          version.previousWriteIfStable
        }
        version.changed = 0
        val res = findNextFrameForSuccessorOperation(version, stabilizeTo)
        if(NonblockingSkipListVersionHistory.TRACE_VALUES) {
          if(maybeValue.isDefined) {
            println(s"[${Thread.currentThread().getName}] reevOut $turn wrote $version => $res")
          } else {
            println(s"[${Thread.currentThread().getName}] reevOut $turn left $version unchanged => $res.")
          }
        }
        res
      }
    }
    result
  }

  @tailrec private def pushStableAndFindNewFirstFrame(current: QueuedVersion, stabilizeTo: QueuedVersion): QueuedVersion = {
    assert(stabilizeTo != null, s"must not stabilize from $current to null.\r\nfirstFrame = $firstFrame\r\nlatestStable = ${laggingLatestStable.get()}")
    assert(stabilizeTo.value != Unwritten, s"must not stabilize from $current to unwritten $stabilizeTo")
    assert(current.pending == 0, s"pushStable from $current must be final (pending)")
    assert(current.changed == 0, s"pushStable from $current must be final (changed)")
    val next = current.get()
    if(next != null && next.txn.phase == TurnPhase.Executing) {
      // next is stable ...
      next.stabilize(stabilizeTo)
      if(next.sleeping) LockSupport.unpark(next.txn.userlandThread)
      if (next.pending == 0 && next.changed == 0) {
        // ... and final
        pushStableAndFindNewFirstFrame(next, stabilizeTo)
      } else {
        // ... but not final (i.e. next is frame, and thus new firstFrame)
        setLatestStable(next)
        next
      }
    } else {
      // next is not stable
      setLatestStable(current)
      // look for an unstable firstFrame
      var newFirstFrame = next
      while(newFirstFrame != null && newFirstFrame.pending == 0 && newFirstFrame.changed == 0){
        newFirstFrame = newFirstFrame.get()
      }
      newFirstFrame
    }
  }

  private def findNextFrameForSuccessorOperation(finalizedVersion: QueuedVersion, stabilizeTo: QueuedVersion): NotificationResultAction.NotificationOutAndSuccessorOperation[T, OutDep] = {
    val newFirstFrame = pushStableAndFindNewFirstFrame(finalizedVersion, stabilizeTo)
    if(NonblockingSkipListVersionHistory.DEBUG) println(s"[${Thread.currentThread().getName}] finalized $finalizedVersion moving firstFrame to $newFirstFrame.")
    firstFrame = newFirstFrame
    if(newFirstFrame != null) {
      if(newFirstFrame.pending == 0) {
        assert(newFirstFrame.changed != 0, s"stabilize stopped at marker $newFirstFrame in $this")
        assert(newFirstFrame.changed > 0, s"no pending but negative changed should be impossible for new firstFrame $newFirstFrame after finalizing $finalizedVersion")
        NotificationResultAction.NotificationOutAndSuccessorOperation.NextReevaluation(outgoings, newFirstFrame.txn)
      } else if(newFirstFrame.pending > 0) {
        NotificationResultAction.NotificationOutAndSuccessorOperation.FollowFraming(outgoings, newFirstFrame.txn)
      } else /* if(maybeNewFirstFrame.pending < 0) */ {
        NotificationResultAction.NotificationOutAndSuccessorOperation.NoSuccessor(outgoings)
      }
    } else {
      NotificationResultAction.NotificationOutAndSuccessorOperation.NoSuccessor(outgoings)
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
    val ls = laggingLatestStable.get()
    if(ls.txn == txn || ls.txn.isTransitivePredecessor(txn)) {
      if(NonblockingSkipListVersionHistory.DEBUG || NonblockingSkipListVersionHistory.TRACE_VALUES) println(s"[${Thread.currentThread().getName}] dynamicBefore dispatches to staticBefore for $txn at/before latestStable $ls")
      staticBefore(txn)
    } else {
      val versionInQueue = enqueueReading(txn, ls)
      if(versionInQueue == null) {
        dynamicBefore(txn)
      } else {
        val maybeStable = versionInQueue.previousWriteIfStable
        val stable = if(maybeStable == null) {
          assert(!versionInQueue.sleeping, s"someone else is sleeping on my version $versionInQueue?")
          versionInQueue.sleeping = true
          sleepForStable(versionInQueue)
        } else {
          casLatestStable(ls, versionInQueue)
          maybeStable
        }
        val res = stable.readForFuture
        if(NonblockingSkipListVersionHistory.TRACE_VALUES) println(s"[${Thread.currentThread().getName}] dynamicBefore for $txn ended on $stable, returning $res")
        res
      }
    }
  }

  @tailrec private def sleepForStable(version: QueuedVersion): QueuedVersion = {
    val stable = version.previousWriteIfStable
    if(stable == null) {
      if(NonblockingSkipListVersionHistory.DEBUG) println(s"[${Thread.currentThread().getName}] suspending for stable $version")
      LockSupport.park(NonblockingSkipListVersionHistory.this)
      sleepForStable(version)
    } else {
      if(NonblockingSkipListVersionHistory.DEBUG) println(s"[${Thread.currentThread().getName}] woke up on stable $version")
      version.sleeping = false
      stable
    }
  }

  def staticBefore(txn: T): V = {
    // start at latest stable
    var predecessor = laggingLatestStable.get
    if(NonblockingSkipListVersionHistory.DEBUG && NonblockingSkipListVersionHistory.TRACE_VALUES) println(s"[${Thread.currentThread().getName}] staticBefore for $txn starting from $predecessor")
    // hop back in time, ignoring all writes that are considered in the future of txn or of txn itself
    while(predecessor.txn == txn || predecessor.txn.isTransitivePredecessor(txn)) {
      predecessor = predecessor.previousWriteIfStable
    }
    assert(txn.isTransitivePredecessor(predecessor.txn) || predecessor.txn.phase == TurnPhase.Completed, s"staticBefore of $txn reading from non-predecessor $predecessor?")
    val res = predecessor.readForFuture
    if(NonblockingSkipListVersionHistory.TRACE_VALUES) println(s"[${Thread.currentThread().getName}] staticBefore for $txn ended at $predecessor, returning $res")
    res
  }

  /**
    * entry point for after(this); may suspend.
    * @param txn the executing transaction
    * @return the corresponding value from after this transaction, i.e., awaiting and returning the
    *         transaction's own write if one has occurred or will occur.
    */
  @tailrec final def dynamicAfter(txn: T): V = {
    val ls = laggingLatestStable.get()
    if(ls.txn == txn || ls.txn.isTransitivePredecessor(txn)) {
      if(NonblockingSkipListVersionHistory.DEBUG || NonblockingSkipListVersionHistory.TRACE_VALUES) println(s"[${Thread.currentThread().getName}] dynamicAfter dispatches to staticAfter for $txn at/before latestStable $ls")
      staticAfter(txn)
    } else {
      val versionInQueue = enqueueReading(txn, ls)
      if(versionInQueue == null) {
        dynamicAfter(txn)
      } else {
        val maybeStable = versionInQueue.previousWriteIfStable
        val stable = if(maybeStable == null) {
          assert(!versionInQueue.sleeping, s"someone else is sleeping on my version $versionInQueue?")
          versionInQueue.sleeping = true
          sleepForStable(versionInQueue)
        } else {
          casLatestStable(ls, versionInQueue)
          maybeStable
        }
        val res = if(versionInQueue.value != Unwritten) {
          versionInQueue.readForSelf
        } else {
          stable.readForFuture
        }
        if(NonblockingSkipListVersionHistory.TRACE_VALUES) println(s"[${Thread.currentThread().getName}] dynamicAfter for $txn ended on $versionInQueue, returning $res")
        res
      }
    }
  }

  def staticAfter(txn: T): V = {
    var ownOrPredecessor = laggingLatestStable.get
    if(NonblockingSkipListVersionHistory.DEBUG && NonblockingSkipListVersionHistory.TRACE_VALUES) println(s"[${Thread.currentThread().getName}] staticAfter for $txn starting from $ownOrPredecessor")
    // start at latest write
    if(ownOrPredecessor.value == Unwritten) ownOrPredecessor = ownOrPredecessor.previousWriteIfStable
    // hop back in time over all writes that are considered in the future of txn
    while(ownOrPredecessor.txn != txn && ownOrPredecessor.txn.isTransitivePredecessor(txn)) {
      ownOrPredecessor = ownOrPredecessor.previousWriteIfStable
    }
    // dispatch read
    val res = if(ownOrPredecessor.txn == txn) {
      ownOrPredecessor.readForSelf
    } else {
      ownOrPredecessor.readForFuture
    }
    if(NonblockingSkipListVersionHistory.TRACE_VALUES) println(s"[${Thread.currentThread().getName}] staticAfter for $txn ended at $ownOrPredecessor, returning $res")
    res
  }

  // =================== DYNAMIC OPERATIONS ====================

  @tailrec private def stableVersionExists(txn: T, current: QueuedVersion): Boolean = {
    if(current == null) {
      // restart from laggingStable
      val ls = laggingLatestStable.get()
      if(ls.txn.isTransitivePredecessor(txn)) {
        // required version was dumped off the list
        true
      } else {
        // move forward
        stableVersionExists(txn, ls)
      }
    } else if(current.txn == txn) {
      // required version was found: should be stable
      current.previousWriteIfStable != null
    } else if(current.pending != 0 || current.changed != 0) {
      // encountered a frame, i.e., everything following isn't stable
      false
    } else {
      val next = current.get()
      if(next == null) {
        // reached end of list: required version not found
        false
      } else if(next == current) {
        // search fell of list: restart
        stableVersionExists(txn, null)
      } else {
        // move forward
        stableVersionExists(txn, next)
      }
    }
  }
  def discover(txn: T, add: OutDep): (List[T], Option[T]) = synchronized {
    // if this is a trailing edge change (i.e., ordered at the end of the history),
    // the required a marker for future writes to order themselves behind it should have been
    // created previously already, by the preceding dynamicAfter of DynamicTicket.depend().
    // should some method be introduced that executes a discover WITHOUT a prior staticAfter,
    // then this method needs the same protection current implemented in drop(txn, x).
    assert(stableVersionExists(txn, null), s"trailing discover without existing version by $txn?")
    computeRetrofit(txn, synchronized {
      outgoings += add
      (laggingLatestStable.get, firstFrame)
    })
  }

  @tailrec private def stableUpToLarger(txn: T, current: QueuedVersion): Boolean = {
    current.previousWriteIfStable != null && (current.txn.isTransitivePredecessor(txn) || stableUpToLarger(txn, current.get()))
  }

  @tailrec private def protectTrailingRead(txn: T): Unit = {
    val ls = laggingLatestStable.get()
    if(ls.txn != txn && !ls.txn.isTransitivePredecessor(txn) && !enqueueFinalIfTrailing(txn, ls)) {
      protectTrailingRead(txn)
    }
  }

  @tailrec final def enqueueFinalIfTrailing(txn: T, current: QueuedVersion): Boolean = {
    val next = current.get
    if(next == current) {
      if(NonblockingSkipListVersionHistory.DEBUG) println(s"[${Thread.currentThread().getName}] enqueueFinal fell of the list at $next")
      false
    } else if (next != null && (next.txn == txn || (next.txn.isTransitivePredecessor(txn) && next.txn.phase == TurnPhase.Executing))) {
      if(NonblockingSkipListVersionHistory.DEBUG) {
        if(next.txn == txn) {
          println(s"[${Thread.currentThread().getName}] enqueueFinal for $txn already exists: $next")
        } else {
          println(s"[${Thread.currentThread().getName}] enqueueFinal for $txn obsolete due to executing successor $next")
        }
      }
      assert({
        val ff = firstFrame
        ff == null || ff.txn.isTransitivePredecessor(txn)
      }, s"dynamic edge change found own or executing successor $next not stable?")
      true
    } else if (next == null || NonblockingSkipListVersionHistory.tryFixSuccessorOrderIfNotFixedYet(txn, next.txn)) {
      if (NonblockingSkipListVersionHistory.tryFixPredecessorOrderIfNotFixedYet(current.txn, txn)) {
        val v = tryInsertVersion(txn, current, next, null)
        if (v != null) {
          if(NonblockingSkipListVersionHistory.DEBUG) println(s"[${Thread.currentThread().getName}] enqueueFinal for $txn inserted trailing marker $v between $current and $next")
          assert({
            val ff = firstFrame
            ff == null || ff.txn.isTransitivePredecessor(txn)
          }, s"dynamic edge change found own $next not stable?")
          true
        } else {
          enqueueFinalIfTrailing(txn, current)
        }
      } else {
        if(NonblockingSkipListVersionHistory.DEBUG) println(s"[${Thread.currentThread().getName}] enqueueFollowFraming failed speculative ordering $current < $txn, and thus became obsolete due to executing successor.")
        assert({
          val ff = firstFrame
          ff == null || ff.txn.isTransitivePredecessor(txn)
        }, s"dynamic edge change found own or executing successor $next not stable?")
        true
      }
    } else {
      enqueueFinalIfTrailing(txn, next)
    }
  }

  def drop(txn: T, remove: OutDep): (List[T], Option[T]) = synchronized {
    protectTrailingRead(txn)
    computeRetrofit(txn, synchronized {
      outgoings -= remove
      (laggingLatestStable.get, firstFrame)
    })
  }

  private def computeRetrofit(txn: T, latestStableAndFirstFrame: (QueuedVersion, QueuedVersion)): (List[T], Option[T]) = {
    val maybeFirstFrame = latestStableAndFirstFrame._2
    val frame = if(maybeFirstFrame != null) {
      assert(maybeFirstFrame.txn.isTransitivePredecessor(txn), s"can only retrofit into the past, but $txn is in the future of firstframe $maybeFirstFrame!")
      Some(maybeFirstFrame.txn)
    } else {
      None
    }

    var logLine = latestStableAndFirstFrame._1
    if(logLine.value == Unwritten || logLine == maybeFirstFrame) logLine = logLine.previousWriteIfStable
    var retrofits: List[T] = Nil
    while(logLine.txn == txn || logLine.txn.isTransitivePredecessor(txn) || (
      logLine.txn.phase != TurnPhase.Completed &&
      !txn.isTransitivePredecessor(logLine.txn) &&
      !NonblockingSkipListVersionHistory.tryRecordRelationship(logLine.txn, txn, logLine.txn, txn)
    )) {
      retrofits ::= logLine.txn
      logLine = logLine.previousWriteIfStable
    }
    (retrofits, frame)
  }

  /**
    * performs the reframings on the sink of a discover(n, this) with arity +1, or drop(n, this) with arity -1
    * @param successorWrittenVersions the reframings to perform for successor written versions
    * @param maybeSuccessorFrame maybe a reframing to perform for the first successor frame
    * @param arity +1 for discover adding frames, -1 for drop removing frames.
    */
  def retrofitSinkFrames(successorWrittenVersions: List[T], maybeSuccessorFrame: Option[T], arity: Int): Unit = synchronized {
    require(math.abs(arity) == 1)
//    @tailrec def checkFrames(current: List[T]): Unit = {
//      current match {
//        case Nil =>
//        case first :: Nil => if(maybeSuccessorFrame.isDefined) {
//          assert(maybeSuccessorFrame.get.isTransitivePredecessor(first), s"not $first < $maybeSuccessorFrame")
//        }
//        case first :: rest =>
//          val second :: _ = rest
//          assert(second.isTransitivePredecessor(current.head), s"not $first < $second")
//          checkFrames(rest)
//      }
//    }
//    checkFrames(successorWrittenVersions)

    var current = firstFrame
    for(txn <- successorWrittenVersions) {
      // current.txn == txn can only occur for successorWrittenVersions.head
      if(current.txn != txn) current = enqueueExecuting(txn, current)
      // note: if drop retrofitting overtook a change notification, changed may update from 0 to -1 here!
      current.changed += arity
    }

    if (maybeSuccessorFrame.isDefined) {
      val txn = maybeSuccessorFrame.get
      // current.txn == txn can only occur if successorWrittenVersions.isEmpty
      val version = if(current.txn == txn) current else enqueueFollowFraming(txn, current)
      // note: conversely, if a (no)change notification overtook discovery retrofitting, pending may change
      // from -1 to 0 here. No handling is required for this case, because firstFrame < position is an active
      // reevaluation (the one that's executing the discovery) and will afterwards progressToNextWrite, thereby
      // executing this then-ready reevaluation, but for now the version is guaranteed not stable yet.
      version.pending += arity
    }
    // cannot make this assertion here because dynamic events might make the firstFrame not a frame when dropping the only incoming changed dependency..
    //assertOptimizationsIntegrity(s"retrofitSinkFrames(writes=$successorWrittenVersions, maybeFrame=$maybeSuccessorFrame)")
  }

  private def tryInsertVersion(txn: T, current: QueuedVersion, next: QueuedVersion, previousWriteIfStable: QueuedVersion): QueuedVersion = {
    assert(current.txn != txn, s"trying to insert duplicate after $current!")
    assert(next == null || next.txn != txn, s"trying to insert duplicate before $current!")
    assert(previousWriteIfStable == null || previousWriteIfStable.value != Unwritten, s"cannot set unwritten stable $previousWriteIfStable")
    assert(txn.isTransitivePredecessor(current.txn) || current.txn.phase == TurnPhase.Completed, s"inserting version for $txn after unordered $current")
    assert(next == null || next.txn.isTransitivePredecessor(txn), s"inserting version for $txn before unordered $next")
    val waiting = new QueuedVersion(txn, previousWriteIfStable, 0, 0, Unwritten, next, false)
    if (current.compareAndSet(next, waiting)) {
      waiting
    } else {
      null
    }
  }
}

object NonblockingSkipListVersionHistory {
  val DEBUG = false
  val TRACE_VALUES = false

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
    * @param succ is attempted to be phase-locked in framing, if not already ordered behind txn
    * @return success if order was or became established, failure if it wasn't and succ started executing
    */
  def tryFixSuccessorOrderIfNotFixedYet(txn: FullMVTurn, succ: FullMVTurn): Boolean = {
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
    * @return success if order was or became established, failure if reverse order was or became established concurrently
    */
  def tryFixPredecessorOrderIfNotFixedYet(pred: FullMVTurn, txn: FullMVTurn): Boolean = {
    txn.isTransitivePredecessor(pred) || pred.phase == TurnPhase.Completed || tryRecordRelationship(pred, txn, pred, txn)
  }
}
