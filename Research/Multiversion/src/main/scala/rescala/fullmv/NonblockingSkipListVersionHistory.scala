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
  class WriteLogElement(val txn: T, val value: V, var pred: WriteLogElement)
  class QueuedVersion(val txn: T, @volatile var finalized: WriteLogElement, var pending: Int, var changed: Int, next: QueuedVersion, @volatile var sleeping: Boolean) extends AtomicReference[QueuedVersion](next)

  // unsynchronized because written sequentially by notify/reevOut
  @volatile var log: WriteLogElement = new WriteLogElement(init, valuePersistency.initialValue, null)
  @volatile var latestValue: V
  // synchronized, written sequentially only if firstFrame.txn.phase == Executing && queueHead == firstFrame by notify/reevOut
  val queueHead = new AtomicReference[QueuedVersion](new QueuedVersion(log.txn, log, 0, 0, null, false))

  def ensureQueuedFraming(txn: T): QueuedVersion
  def ensureQueuedNotifying(txn: T): QueuedVersion
  def ensureQueuedFollowFraming(txn: T): QueuedVersion

  // =================== STORAGE ====================

  // unsynchronized because written sequentially through object monitor
  @volatile var firstFrame = null.asInstanceOf[QueuedVersion]
  // unsynchronized because written AND read sequentially by notify/reevOut
  var laggingGC = log

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
    val version = ensureQueuedFraming(txn)
    val result = synchronized { incrementFrame0(version) }
    result
  }

  /**
    * entry point for superseding framing
    * @param txn the transaction visiting the node for framing
    * @param supersede the transaction whose frame was superseded by the visiting transaction at the previous node
    */
  def incrementSupersedeFrame(txn: T, supersede: T): FramingBranchResult[T, OutDep] = {
    val version = ensureQueuedFraming(txn)
    val supersedeVersion = ensureQueuedFraming(supersede/*TODO, version*/)
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
    val version = ensureQueuedNotifying(txn)
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
    val version = ensureQueuedNotifying(txn)
    val followFrame = ensureQueuedFollowFraming(txn/* TODO, version*/)
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

  @tailrec final def pushFinalized(current: QueuedVersion): QueuedVersion = {
    current.finalized = log
    val next = current.get()
    if(next.get != null && next.pending == 0 && next.changed == 0 && next.txn.phase == TurnPhase.Executing) {
      pushFinalized(next)
    } else {
      queueHead.set(current)
      current
    }
  }

  private def findNextFrameForSuccessorOperation(/*finalizedVersion: Version, stabilizeTo: Version*/): NotificationResultAction.NotificationOutAndSuccessorOperation[T, OutDep] = {
    val finalized = pushFinalized(firstFrame)

    var newFirstFrame = finalized.get()
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
    @tailrec def traverseQueue(txn: T, current: QueuedVersion): V = {
      val f = current.finalized
      val next = current.get
      if (next != null && f != null && next.pending == 0 && next.changed == 0 && next.txn.phase == TurnPhase.Executing && next.finalized == null) {
        next.finalized = f
      }
      if (next != null && next.txn == txn) {
        if (f != null) {
          f.value
        } else {
          assert(!next.sleeping, s"someone's sleeping on my version!")
          next.sleeping = true
          waitForStableThenReadBefore(current, next)
        }
      } else if (next == null || NonblockingSkipListVersionHistory.tryFixSuccessorOrder(txn, next.txn)) {
        if(NonblockingSkipListVersionHistory.tryFixPredecessorOrder(current.txn, txn)) {
          if (f != null) {
            f.value
          } else {
            val waiting = new QueuedVersion(txn, null, 0, 0, next, true)
            if (current.compareAndSet(next, waiting)) {
              waitForStableThenReadBefore(current, waiting)
            } else {
              traverseQueue(txn, current)
            }
          }
        } else {
          // restart
          val head = queueHead.get()
          if(head.txn == txn || head.txn.isTransitivePredecessor(txn)) {
            staticBefore(txn)
          } else {
            traverseQueue(txn, head)
          }
        }
      } else {
        traverseQueue(txn, next)
      }
    }
    // start
    val head = queueHead.get()
    if(head.txn == txn || head.txn.isTransitivePredecessor(txn)) {
      staticBefore(txn)
    } else {
      traverseQueue(txn, head)
    }
  }

  def staticBefore(txn: T): V = {
    var predecessor = log
    while(predecessor.txn == txn || predecessor.txn.isTransitivePredecessor(txn)) {
      predecessor = predecessor.pred
    }
    predecessor.value
  }

  /**
    * entry point for after(this); may suspend.
    * @param txn the executing transaction
    * @return the corresponding value from after this transaction, i.e., awaiting and returning the
    *         transaction's own write if one has occurred or will occur.
    */
  def dynamicAfter(txn: T): V = {
    @tailrec def traverseQueue(txn: T, current: QueuedVersion): V = {
      val f = current.finalized
      val next = current.get
      if (next != null && f != null && next.pending == 0 && next.changed == 0 && next.txn.phase == TurnPhase.Executing && next.finalized == null) {
        next.finalized = f
      }
      if (next != null && next.txn == txn) {
        if (f != null) {
          f.value
        } else {
          assert(!next.sleeping, s"someone's sleeping on my version!")
          next.sleeping = true
          waitForStableThenReadBefore(current, next)
        }
      } else if (next == null || NonblockingSkipListVersionHistory.tryFixSuccessorOrder(txn, next.txn)) {
        if(NonblockingSkipListVersionHistory.tryFixPredecessorOrder(current.txn, txn)) {
          if (f != null) {
            f.value
          } else {
            val waiting = new QueuedVersion(txn, null, 0, 0, next, true)
            if (current.compareAndSet(next, waiting)) {
              waitForStableThenReadBefore(current, waiting)
            } else {
              traverseQueue(txn, current)
            }
          }
        } else {
          // restart
          val head = queueHead.get()
          if(head.txn == txn || head.txn.isTransitivePredecessor(txn)) {
            staticAfter(txn)
          } else {
            traverseQueue(txn, head)
          }
        }
      } else {
        traverseQueue(txn, next)
      }
    }
    // start
    val head = queueHead.get()
    if(head.txn == txn || head.txn.isTransitivePredecessor(txn)) {
      staticAfter(txn)
    } else {
      traverseQueue(txn, head)
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
      (log, firstFrame)
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
      (log, firstFrame)
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

trait DedicatedSearchStrategies[V, T <: FullMVTurn, InDep, OutDep] extends NonblockingSkipListVersionHistory[V, T, InDep, OutDep] {
  override def ensureQueuedFraming(txn: T): QueuedVersion = {
    @tailrec def tryQueue(current: QueuedVersion): QueuedVersion = {
      val next = current.get()
      if(next == null || next.txn.isTransitivePredecessor(txn) /* always go as far back as possible */) {
        if(NonblockingSkipListVersionHistory.tryFixPredecessorOrder(current.txn, txn)) {
          val q = new QueuedVersion(txn, null, 0, 0, next, false)
          if(current.compareAndSet(next, q)) {
            q
          } else {
            tryQueue(current)
          }
        } else {
          // restart
          /* more optimal: go backwards, but we don't have backwards pointer (yet?) */
          tryQueue(queueHead.get)
        }
      } else {
        tryQueue(next)
      }
    }
    // start
    tryQueue(queueHead.get)
  }

  override def ensureQueuedNotifying(txn: T): QueuedVersion = {
    @tailrec def tryQueue(current: QueuedVersion): QueuedVersion = {
      val next = current.get()
      if(next == null || NonblockingSkipListVersionHistory.tryFixSuccessorOrder(txn, next.txn) /* go back only until success in locking a txn in framing*/) {
        if(NonblockingSkipListVersionHistory.tryFixPredecessorOrder(current.txn, txn)) {
          val q = new QueuedVersion(txn, null, 0, 0, next, false)
          if(current.compareAndSet(next, q)) {
            q
          } else {
            tryQueue(current)
          }
        } else {
          // restart
          /* more optimal: go backwards, but we don't have backwards pointer (yet?) */
          tryQueue(queueHead.get)
        }
      } else {
        tryQueue(next)
      }
    }
    // start
    tryQueue(queueHead.get)
  }

  override def ensureQueuedFollowFraming(txn: T): QueuedVersion = {
    @tailrec def tryQueue(current: QueuedVersion): QueuedVersion = {
      val next = current.get()
      if(next == null || next.txn.isTransitivePredecessor(txn) /* assume we are still Framing, thus always go as far back as possible */) {
        if(txn.isTransitivePredecessor(current.txn) || (current.txn.phase match {
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
          if(current.compareAndSet(next, q)) {
            q
          } else {
            tryQueue(current)
          }
        } else {
          // restart
          if(txn.phase == TurnPhase.Executing /* actually check if we are Executing just once at the beginning */) {
            ensureQueuedNotifying(txn)
          } else {
            /* more optimal: go backwards, but we don't have backwards pointer (yet?) */
            tryQueue(queueHead.get)
          }
        }
      } else {
        tryQueue(next)
      }
    }
    // start
    if(txn.phase == TurnPhase.Executing /* actually check if we are Executing just once at the beginning */) {
      ensureQueuedNotifying(txn)
    } else {
      tryQueue(queueHead.get)
    }
  }
}

trait SingleGenericSearchAlgorithm[V, T <: FullMVTurn, InDep, OutDep] extends NonblockingSkipListVersionHistory[V, T, InDep, OutDep] {
  override def ensureQueuedFraming(txn: T): QueuedVersion = ensureQueued(txn)
  override def ensureQueuedNotifying(txn: T): QueuedVersion = ensureQueued(txn)
  override def ensureQueuedFollowFraming(txn: T): QueuedVersion = ensureQueued(txn)

  def ensureQueued(txn: T): QueuedVersion = {
    @tailrec def tryQueue(current: QueuedVersion): QueuedVersion = {
      val next = current.get()
      if(next == null || next.txn.isTransitivePredecessor(txn) || (txn.phase == TurnPhase.Executing && {
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
            tryQueue(current)
          }
        } else {
          // restart
          /* more optimal: go backwards, but we don't have backwards pointer (yet?) */
          tryQueue(queueHead.get)
        }
      } else {
        tryQueue(next)
      }
    }
    // start
    tryQueue(queueHead.get)
  }
}

object NonblockingSkipListVersionHistory {
  /**
    * @param attemptPredecessor
    * @param succToRecord
    * @param defender
    * @param contender
    * @return true relation is final (recorded by self or concurrent thread, or predecessor completed), false if reverse relation was recorded concurrently
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

  def tryFixPredecessorOrder(pred: FullMVTurn, txn: FullMVTurn): Boolean = {
    txn.isTransitivePredecessor(pred) || pred.phase == TurnPhase.Completed || tryRecordRelationship(pred, txn, pred, txn)
  }
}
