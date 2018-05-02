//package rescala.fullmv
//
//import java.util.concurrent.atomic.AtomicReference
//import java.util.concurrent.locks.LockSupport
//
//import rescala.core.Initializer.InitValues
//import rescala.fullmv.FramingBranchResult.FramingBranchEnd
//import rescala.fullmv.NodeVersionHistory._
//
//import scala.annotation.elidable.ASSERTION
//import scala.annotation.{elidable, tailrec}
//import scala.collection.mutable.ArrayBuffer
//
///**
//  * A node version history datastructure
//  * @param init the initial creating transaction
//  * @param valuePersistency the value persistency descriptor
//  * @tparam V the type of stored values
//  * @tparam T the type of transactions
//  * @tparam InDep the type of incoming dependency nodes
//  * @tparam OutDep the type of outgoing dependency nodes
//  */
//class NonblockingSkipListVersionHistory[V, T <: FullMVTurn, InDep, OutDep](init: T, val valuePersistency: InitValues[V]) {
//  class Version(val txn: T, val next: AtomicReference[Version], @volatile var lastWrittenPredecessorIfStable: Version, var out: Set[OutDep], var pending: Int, var changed: Int, @volatile var value: Option[V]) /*extends MyManagedBlocker*/ {
//    // txn >= Executing, stable == true, node reevaluation completed changed
//    def isWritten: Boolean = changed == 0 && value.isDefined
//    // txn <= WrapUp, any following versions are stable == false
//    def isFrame: Boolean = pending > 0 || changed > 0
//    // isReadOrDynamic: has no implications really..
//    def isReadOrDynamic: Boolean = pending == 0 && changed == 0 && value.isEmpty
//    // isOvertakeCompensation: Will become isReadOrDynamic or isFrame once overtaken (no)change notifications have arrived.
//    def isOvertakeCompensation: Boolean = pending < 0 || changed < 0
//
//    // should only be used if isFrame == true is known (although it implies that)
//    def isReadyForReevaluation: Boolean = pending == 0 && changed > 0
//    // should only be used if txn >= Executing, as it may falsely return true in the case that a txn == Framing
//    // had a frame converted into a marker due to frame superseding (or drop retrofitting?) and that frame was
//    // marked stable after all preceding placeholders were removed but anoter txn2 == Framing inserts another
//    // preceding frame which destabilizes this version again.
//    def isStable: Boolean = lastWrittenPredecessorIfStable != null
//    // should only be used if txn >= Executing, as it may falsely return true in the case that a txn == Framing
//    // had a frame converted into a marker due to frame superseding (or drop retrofitting?) and that frame was
//    // marked stable after all preceding placeholders were removed but anoter txn2 == Framing inserts another
//    // preceding frame which destabilizes this version again.
//    def isFinal: Boolean = isWritten || (isReadOrDynamic && isStable)
//
//    def read(): V = {
//      assert(isWritten, "reading un-written "+this)
//      value.get
//    }
//
//    @volatile var stableWaiters: Int = 0
//
//    // less common blocking case
//    // fake lazy val without synchronization, because it is accessed only while the node's monitor is being held.
//    def blockForStable(): Unit = {
//      if (!isStable) {
//        stableWaiters += 1
//        assert(Thread.currentThread() == txn.userlandThread, s"this assertion is only valid without a threadpool .. otherwise it should be txn==txn, but that would require txn to be spliced in here which is annoying while using the managedblocker interface")
//        while (!isStable) {
//          if (FullMVEngine.DEBUG) println(s"[${Thread.currentThread().getName}] parking for stable ${Version.this}")
//          LockSupport.park(this)
//          if (FullMVEngine.DEBUG) println(s"[${Thread.currentThread().getName}] unparked on ${Version.this}")
//        }
//        stableWaiters -= 1
//      }
//    }
//
//    override def toString: String = {
//      if(isWritten){
//        s"Written($txn, out=$out, v=${value.get})"
//      } else if (isReadOrDynamic) {
//        (if(isStable) "Stable" else "Unstable") + s"Marker($txn, out=$out)"
//      } else if (isOvertakeCompensation) {
//        s"OvertakeCompensation($txn, ${if (isStable) "stable" else "unstable"}, out=$out, pending=$pending, changed=$changed)"
//      } else if(isFrame) {
//        if(isStable) {
//          if(isReadyForReevaluation) {
//            s"Active($txn, out=$out)"
//          } else {
//            s"FirstFrame($txn, out=$out, pending=$pending, changed=$changed)"
//          }
//        } else {
//          if(isReadyForReevaluation) {
//            s"Queued($txn, out=$out)"
//          } else {
//            s"Frame($txn, out=$out, pending=$pending, changed=$changed)"
//          }
//        }
//      } else {
//        "UnknownVersionCase!(" + txn + ", " + (if(isStable) "stable" else "unstable") + ", out=" + out + ", pending=" + pending + ", changed=" + changed + ", value = " + value + ")"
//      }
//    }
//  }
//
//  // =================== STORAGE ====================
//
//  val head = new AtomicReference(new Version(init, new AtomicReference(null), lastWrittenPredecessorIfStable = null, out = Set(), pending = 0, changed = 0, Some(valuePersistency.initialValue)))
//  val firstFrame = new AtomicReference[Version](null)
//  val tail = new AtomicReference(head.get)
////  val latestKnownStable = new AtomicReference(head.get)
//
//  var latestValue: V = valuePersistency.initialValue
//  var incomings: Set[InDep] = Set.empty
//
//  sealed trait FindMaxResult
//  case class Found(version: Version) extends FindMaxResult
//  case class NotFound(orderedBefore: Version, orderedAfter: Version) extends FindMaxResult
//
//  // =================== FRAMING SEARCH AND INSERT ===================== //
//  @tailrec private def findMaxUpToFraming(lkh: Version, txn: T, current: Version, newHead: Boolean): FindMaxResult = {
//    val next = current.next.get
//    if(next.txn == txn) {
//      if(newHead) helpGC(lkh, current)
//      Found(next)
//    } else if (next == null || next.txn.isTransitivePredecessor(txn)) {
//      if(newHead){
//        helpGC(lkh, current)
//        NotFound(current, next)
//      } else {
//        if(txn.isTransitivePredecessor(current.txn) || tryRecordRelationship(current.txn, txn, current.txn, txn)) {
//          NotFound(current, next)
//        } else {
//          // reverse relation was recorded
//          val lkh = head.get()
//          findMaxUpToFraming(lkh, txn, lkh, newHead = false)
//        }
//      }
//    } else next.txn.phase match {
//      case TurnPhase.Completed =>
//        findMaxUpToFraming(lkh, txn, next, newHead = true)
//      case TurnPhase.Executing =>
//        if(newHead) helpGC(lkh, current)
//        findMaxUpToFraming(lkh, txn, next, newHead = false)
//      case TurnPhase.Framing =>
//        if(newHead) helpGC(lkh, current)
//        findMaxUpToFraming(lkh, txn, next, newHead = false)
//      case otherwise => throw new AssertionError(s"unexpected phase $otherwise in version search for $txn from $next")
//    }
//  }
//  private def findMaxUpToFraming(txn: T) = {
//    val lkh = head.get()
//    findMaxUpToFraming(lkh, txn, lkh, newHead = false)
//  }
//
//  @tailrec private def ensureVersionFraming(txn: T): Version = {
//    tryEnsureVersion(txn, findMaxUpToFraming(txn)) match {
//      case null => ensureVersionFraming(txn)
//      case v => v
//    }
//  }
//
//  // =================== GENEARL SEARCH AND INSERT ===================== //
//
//  /**
//    * @param attemptPredecessor
//    * @param succToRecord
//    * @param defender
//    * @param contender
//    * @return true relation is final (recorded by self or concurrent thread, or predecessor completed), false if reverse relation was recorded concurrently
//    */
//  private def tryRecordRelationship(attemptPredecessor: T, succToRecord: T, defender: T, contender: T): Boolean = {
//    SerializationGraphTracking.tryLock(defender, contender, UnlockedUnknown) match {
//      case x: LockedSameSCC =>
//        try {
//          if (succToRecord.isTransitivePredecessor(attemptPredecessor)) {
//            // relation already recorded
//            true
//          } else if (attemptPredecessor.isTransitivePredecessor(succToRecord)) {
//            // reverse relation already recorded
//            false
//          } else {
//            val tree = attemptPredecessor.selfNode
//            if (tree == null) {
//              assert(attemptPredecessor.phase == TurnPhase.Completed, s"$attemptPredecessor selfNode was null but isn't completed?")
//              // relation no longer needs recording because predecessor completed concurrently
//              true
//            } else {
//              succToRecord.addPredecessor(tree)
//              // relation newly recorded
//              true
//            }
//          }
//        } finally {
//          x.unlock()
//        }
//      case otherwise =>
//        Thread.`yield`()
//        if (attemptPredecessor.phase == TurnPhase.Completed) {
//          // relation no longer needs recording because predecessor completed concurrently
//          true
//        } else if(succToRecord.isTransitivePredecessor(attemptPredecessor)) {
//          // relation already recorded
//          true
//        } else if (attemptPredecessor.isTransitivePredecessor(succToRecord)) {
//          // reverse relation already recorded
//          false
//        } else {
//          // retry
//          tryRecordRelationship(attemptPredecessor, succToRecord, defender, contender)
//        }
//    }
//  }
//
//  private def tryEnsureVersion(txn: T, findMaxResult: FindMaxResult): Version = {
//    findMaxResult match {
//      case Found(v) => v
//      case NotFound(pred, succ) =>
//        val v = new Version(txn, new AtomicReference(succ), computeSuccessorWrittenPredecessorIfStable(pred), pred.out, pending = 0, changed = 0, value = None)
//        if(pred.next.compareAndSet(succ, v)) {
//          // TODO ensure correct stable and out
//          v
//        } else {
//          null
//        }
//    }
//  }
//
//  private def helpGC(lkh: Version, current: Version): Unit = {
//    assert(lkh ne current, s"initial value for traversal should start with newHead false")
//    if(head.compareAndSet(lkh, current)){
//      current.lastWrittenPredecessorIfStable.lastWrittenPredecessorIfStable = null
//    }
//  }
//
//  // =================== EXECUTING SEARCH AND INSERT ===================== //
//
//  @tailrec def findMaxUpToExecuting(lkh: Version, txn: T, current: Version, newHead: Boolean): FindMaxResult = {
//    val next = current.next.get
//    if(next.txn == txn) {
//      if(newHead) helpGC(lkh, current)
//      Found(next)
//    } else if (next == null || next.txn.isTransitivePredecessor(txn)) {
//      if(newHead){
//        helpGC(lkh, current)
//        NotFound(current, next)
//      } else {
//        if(txn.isTransitivePredecessor(current.txn) || tryRecordRelationship(current.txn, txn, current.txn, txn)) {
//          NotFound(current, next)
//        } else {
//          // reverse relation was recorded
//          val lkh = head.get()
//          findMaxUpToExecuting(lkh, txn, lkh, newHead = false)
//        }
//      }
//    } else next.txn.phase match {
//      case TurnPhase.Completed =>
//        findMaxUpToExecuting(lkh, txn, next, newHead = true)
//      case TurnPhase.Executing =>
//        if(newHead) helpGC(lkh, current)
//        findMaxUpToExecuting(lkh, txn, next, newHead = false)
//      case TurnPhase.Framing =>
//        if(newHead) helpGC(lkh, current)
//        next.txn.acquirePhaseLockIfAtMost(TurnPhase.Framing) match {
//          case TurnPhase.Completed =>
//            findMaxUpToExecuting(lkh, txn, next, newHead = true)
//          case TurnPhase.Executing =>
//            if (newHead) helpGC(lkh, current)
//            findMaxUpToExecuting(lkh, txn, next, newHead = false)
//          case TurnPhase.Framing =>
//            try {
//              // order successor
//              if (!next.txn.isTransitivePredecessor(txn)) {
//                val recorded = tryRecordRelationship(txn, next.txn, next.txn, txn)
//                assert(recorded, s"tryRecord should be impossible to fail here because ${next.txn} is phase-locked to a lower phase than $txn")
//              }
//              // try order predecessor
//              if(txn.isTransitivePredecessor(current.txn) || tryRecordRelationship(current.txn, txn, current.txn, txn)) {
//                NotFound(current, next)
//              } else {
//                // reverse relation was recorded
//                val lkh = head.get()
//                findMaxUpToExecuting(lkh, txn, lkh, newHead = false)
//              }
//            } finally {
//              next.txn.asyncReleasePhaseLock()
//            }
//          case otherwise =>
//            if (otherwise <= TurnPhase.Framing) next.txn.asyncReleasePhaseLock()
//            throw new AssertionError(s"phase-locking ${next.txn} returned unhandled phase $otherwise")
//        }
//      case otherwise => throw new AssertionError(s"unexpected phase $otherwise in version search for $txn from $next")
//    }
//  }
//  private def findMaxUpToExecuting(txn: T) = {
//    val lkh = head.get()
//    findMaxUpToExecuting(lkh, txn, lkh, newHead = false)
//  }
//
//  @tailrec private def ensureVersionExecuting(txn: T): Version = {
//    tryEnsureVersion(txn, findMaxUpToExecuting(txn)) match {
//      case null => ensureVersionExecuting(txn)
//      case v => v
//    }
//  }
//
//  // =================== FIRST FRAME MANAGEMENT ===================== //
//
//  // needs to be executed while version.synchronized!
//  @tailrec private def frameCreated(version: Version): FramingBranchResult[T, OutDep] = {
//    val oldFirstFrame = firstFrame.get
//    if(oldFirstFrame == null) {
//      if(firstFrame.compareAndSet(null, version)) {
//        FramingBranchResult.Frame(version.out, version.txn)
//      } else {
//        frameCreated(version)
//      }
//    } else if((oldFirstFrame ne version) && oldFirstFrame.txn.isTransitivePredecessor(version.txn)) {
//      if (firstFrame.compareAndSet(oldFirstFrame, version)) {
//        FramingBranchResult.FrameSupersede(version.out, version.txn, oldFirstFrame.txn)
//      } else {
//        frameCreated(version)
//      }
//    } else {
//      FramingBranchResult.FramingBranchEnd
//    }
//  }
//
//  @tailrec private def firstFrameRemoved(version: Version, out: Set[OutDep]): FramingBranchResult[T, OutDep] = {
//    // this first cas needs to happen still inside version.synchronized i believe..
//    if(firstFrame.compareAndSet(version, null)) {
//      @tailrec def findReframe(current: Version): FramingBranchResult[T, OutDep] = {
//        if (current == null) {
//          FramingBranchResult.Deframe(out, version.txn)
//        } else {
//          val maybeResult = current.synchronized {
//            if (current.isFrame) {
//              @tailrec def tryReframe(): FramingBranchResult[T, OutDep] = {
//                val oldFirstFrame = firstFrame.get
//                if (oldFirstFrame == null) {
//                  if (firstFrame.compareAndSet(null, current)) {
//                    FramingBranchResult.DeframeReframe(out, version.txn, current.txn)
//                  } else {
//                    tryReframe()
//                  }
//                } else if ((oldFirstFrame ne current) && oldFirstFrame.txn.isTransitivePredecessor(current.txn)) {
//                  if (firstFrame.compareAndSet(oldFirstFrame, current)) {
//                    // TODO now need to triple: deframe version.txn, reframe nextFrame.txn, AND deframe oldFirstFrame.txn ?!
//                  } else {
//                    tryReframe()
//                  }
//                } else {
//                  FramingBranchResult.Deframe(out, version.txn)
//                }
//              }
//              tryReframe()
//            } else {
//              null
//            }
//          }
//          if (maybeResult == null) {
//            findReframe(current.next.get)
//          } else {
//            maybeResult
//          }
//        }
//      }
//      findReframe(version.next.get)
//    } else {
//      FramingBranchResult.FramingBranchEnd
//    }
//
//    val oldFirstFrame = firstFrame.get
//    if(oldFirstFrame ne version) {
//      if(firstFrame.compareAndSet(oldFirstFrame, version)) {
//        if(oldFirstFrame == null) {
//          FramingBranchResult.Frame(out, version.txn)
//        } else {
//          FramingBranchResult.FrameSupersede(out, version.txn, oldFirstFrame.txn)
//        }
//      } else {
//        firstFrameCreated(version, out)
//      }
//    } else {
//      FramingBranchResult.FramingBranchEnd
//    }
//  }
//
//  private def computeSuccessorWrittenPredecessorIfStable(predVersion: Version) = {
//    if (predVersion.isFrame) {
//      null
//    } else if (predVersion.isWritten) {
//      predVersion
//    } else {
//      predVersion.lastWrittenPredecessorIfStable
//    }
//  }
//
//
//  // =================== FRAMING ====================
//
//  /**
//    * entry point for regular framing
//    *
//    * @param txn the transaction visiting the node for framing
//    */
//  def incrementFrame(txn: T): FramingBranchResult[T, OutDep] = synchronized {
//    val version = ensureVersionFraming(txn)
//    val result = incrementFrame0(version)
//    result
//  }
//
//  /**
//    * entry point for superseding framing
//    * @param txn the transaction visiting the node for framing
//    * @param supersede the transaction whose frame was superseded by the visiting transaction at the previous node
//    */
//  def incrementSupersedeFrame(txn: T, supersede: T): FramingBranchResult[T, OutDep] = {
//    val version = ensureVersionFraming(txn)
//    val succVersion = ensureVersionFraming(supersede) // TODO search from version on only?
//    version.synchronized {
//      version.pending += 1
//      if (version.pending == 1) {
//        // since this is a frame, successor can't be firstFrame
//        frameCreated(version)
//      } else if(version.pending <= 1) {
//        AlmostFramingBranchResult.FirstFrameUnknown
//      } else {
//        // since this is a frame, successor can't be firstFrame
//        succVersion.pending -= 1
//        FramingBranchEnd
//      }
//    } match {
//      case AlmostFramingBranchResult.FirstFrameUnknown =>
//        decrementFrame0(succVersion)
//      case otherwise: FramingBranchResult[T, OutDep] =>
//        succVersion.synchronized{
//          succVersion.pending -= 1
//        }
//        otherwise
//    }
//  }
//
//  def decrementFrame(txn: T): FramingBranchResult[T, OutDep] = synchronized {
//    val version = ensureVersionFraming(txn)
//    val result = decrementFrame0(version)
//    result
//  }
//
//  def decrementReframe(txn: T, reframe: T): FramingBranchResult[T, OutDep] = synchronized {
//    val version = ensureVersionFraming(txn)
//    val succVersion = ensureVersionFraming(reframe) // TODO search from version on only?
//    version.synchronized {
//      version.pending -= 1
//      if (version.pending == 0 && firstFrame.get == version) {
//        // since this is a frame, successor can't be firstFrame
//        firstFrameRemoved(version)
//      } else if(version.pending <= 1) {
//        AlmostFramingBranchResult.FirstFrameUnknown
//      } else {
//        // since this is a frame, successor can't be firstFrame
//        FramingBranchEnd
//      }
//    } match {
//      case AlmostFramingBranchResult.FirstFrameUnknown =>
//        decrementFrame0(succVersion)
//      case otherwise: FramingBranchResult[T, OutDep] =>
//        succVersion.synchronized{
//          succVersion.pending -= 1
//        }
//        otherwise
//    }
//  }
//
//  private def incrementFrame0(version: Version): FramingBranchResult[T, OutDep] = version.synchronized {
//    version.pending += 1
//    if(version.pending == 1) {
//      frameCreated(version)
//    } else {
//      FramingBranchResult.FramingBranchEnd
//    }
//  }
//
//  private def decrementFrame0(version: Version): FramingBranchResult[T, OutDep] = version.synchronized {
//    version.pending -= 1
//    if (version.pending == 0) {
//      firstFrameRemoved(version, version.out)
//      deframeResultAfterPreviousFirstFrameWasRemoved(txn, version)
//    } else {
//      FramingBranchResult.FramingBranchEnd
//    }
//  }
//
//  @tailrec private def destabilizeBackwardsUntilFrame(): Unit = {
//    if(firstFrame < size) {
//      val version = _versions(firstFrame)
//      assert(version.isStable, s"cannot destabilize $firstFrame: $version")
//      version.lastWrittenPredecessorIfStable = null
//    }
//    firstFrame -= 1
//    if(!_versions(firstFrame).isFrame) destabilizeBackwardsUntilFrame()
//  }
//
//  private def incrementFrameResultAfterNewFirstFrameWasCreated(txn: T, position: Int) = {
//    val previousFirstFrame = firstFrame
//    destabilizeBackwardsUntilFrame()
//    assert(firstFrame == position, s"destablizeBackwards did not reach $position: ${_versions(position)} but stopped at $firstFrame: ${_versions(firstFrame)}")
//
//    if(previousFirstFrame < size) {
//      FramingBranchResult.FrameSupersede(_versions(position).out, txn, _versions(previousFirstFrame).txn)
//    } else {
//      FramingBranchResult.Frame(_versions(position).out, txn)
//    }
//  }
//
//  @tailrec private def stabilizeForwardsUntilFrame(stabilizeTo: Version): Unit = {
//    firstFrame += 1
//    if (firstFrame < size) {
//      val stabilized = _versions(firstFrame)
//      assert(!stabilized.isStable, s"cannot stabilize $firstFrame: $stabilized")
//      stabilized.lastWrittenPredecessorIfStable = stabilizeTo
//      if(stabilized.stableWaiters > 0) {
//        if (FullMVEngine.DEBUG) println(s"[${Thread.currentThread().getName}] unparking ${stabilized.txn.userlandThread.getName} after stabilized $stabilized.")
//        LockSupport.unpark(stabilized.txn.userlandThread)
//      }
//      if (!stabilized.isFrame) stabilizeForwardsUntilFrame(stabilizeTo)
//    }
//  }
//
//  private def deframeResultAfterPreviousFirstFrameWasRemoved(txn: T, version: Version) = {
//    stabilizeForwardsUntilFrame(version)
//    if(firstFrame < size) {
//      FramingBranchResult.DeframeReframe(version.out, txn, _versions(firstFrame).txn)
//    } else {
//      FramingBranchResult.Deframe(version.out, txn)
//    }
//  }
//
//  /*
//   * =================== NOTIFICATIONS/ / REEVALUATION ====================
//   */
//
//  /**
//    * entry point for change/nochange notification reception
//    * @param txn the transaction sending the notification
//    * @param changed whether or not the dependency changed
//    */
//  def notify(txn: T, changed: Boolean): NotificationResultAction[T, OutDep] = synchronized {
//    val result = notify0(getFramePositionPropagating(txn), txn, changed)
//    assertOptimizationsIntegrity(s"notify($txn, $changed) -> $result")
//    result
//  }
//
//  /**
//    * entry point for change/nochange notification reception with follow-up framing
//    * @param txn the transaction sending the notification
//    * @param changed whether or not the dependency changed
//    * @param followFrame a transaction for which to create a subsequent frame, furthering its partial framing.
//    */
//  def notifyFollowFrame(txn: T, changed: Boolean, followFrame: T): NotificationResultAction[T, OutDep] = synchronized {
//    val (pos, followPos) = getFramePositionsPropagating(txn, followFrame)
//    _versions(followPos).pending += 1
//    val result = notify0(pos, txn, changed)
//    assertOptimizationsIntegrity(s"notifyFollowFrame($txn, $changed, $followFrame) -> $result")
//    result
//  }
//
//  private def notify0(position: Int, txn: T, changed: Boolean): NotificationResultAction[T, OutDep] = {
//    val version = _versions(position)
//    // This assertion is probably pointless as it only verifies a subset of assertStabilityIsCorrect, i.e., if this
//    // would fail, then assertStabilityIsCorrect will have failed at the end of the previous operation already.
//    assert((position == firstFrame) == version.isStable, s"firstFrame and stable diverted in $this")
//
//    // note: if the notification overtook a previous turn's notification with followFraming for this transaction,
//    // pending may update from 0 to -1 here
//    version.pending -= 1
//    if (changed) {
//      // note: if drop retrofitting overtook the change notification, change may update from -1 to 0 here!
//      version.changed += 1
//    }
//
//    // check if the notification triggers subsequent actions
//    if (version.pending == 0) {
//      if (position == firstFrame) {
//        if (version.changed > 0) {
//          NotificationResultAction.GlitchFreeReady
//        } else {
//          // ResolvedFirstFrameToUnchanged
//          progressToNextWriteForNotification(version, version.lastWrittenPredecessorIfStable)
//        }
//      } else {
//        if (version.changed > 0) {
//          NotificationResultAction.GlitchFreeReadyButQueued
//        } else {
//          NotificationResultAction.ResolvedNonFirstFrameToUnchanged
//        }
//      }
//    } else {
//      NotificationResultAction.NotGlitchFreeReady
//    }
//  }
//
//  def reevIn(turn: T): V = {
//    assert(synchronized {_versions(firstFrame).txn == turn }, s"$turn called reevIn, but is not first frame owner in $this")
//    latestValue
//  }
//
//  /**
//    * progress [[firstFrame]] forward until a [[Version.isFrame]] is encountered, and
//    * return the resulting notification out (with reframing if subsequent write is found).
//    */
//  def reevOut(turn: T, maybeValue: Option[V]): NotificationResultAction.ReevOutResult[T, OutDep] = synchronized {
//    val position = firstFrame
//    val version = _versions(position)
//    assert(version.txn == turn, s"$turn called reevDone, but first frame is $version (different transaction)")
//    assert(!version.isWritten, s"$turn cannot write twice: $version")
//
//    val result = if(version.pending != 0) {
//      NotificationResultAction.Glitched
//    } else {
//      assert((version.isFrame && version.isReadyForReevaluation) || (maybeValue.isEmpty && version.isReadOrDynamic), s"$turn cannot write changed=${maybeValue.isDefined} in $this")
//      version.changed = 0
//      latestKnownStable = position
//      val stabilizeTo = if (maybeValue.isDefined) {
//        latestValue = valuePersistency.unchange.unchange(maybeValue.get)
//        version.value = maybeValue
//        version
//      } else {
//        version.lastWrittenPredecessorIfStable
//      }
//      progressToNextWriteForNotification(version, stabilizeTo)
//    }
//    assertOptimizationsIntegrity(s"reevOut($turn, ${maybeValue.isDefined}) -> $result")
//    result
//  }
//
//  /**
//    * progresses [[firstFrame]] forward until a [[Version.isFrame]] is encountered and assemble all necessary
//    * information to send out change/nochange notifications for the given transaction. Also capture synchronized,
//    * whether or not the possibly encountered write [[Version.isReadyForReevaluation]].
//    * @return the notification and next reevaluation descriptor.
//    */
//  private def progressToNextWriteForNotification(finalizedVersion: Version, stabilizeTo: Version): NotificationResultAction.NotificationOutAndSuccessorOperation[T, OutDep] = {
//    stabilizeForwardsUntilFrame(stabilizeTo)
//    val res = if(firstFrame < size) {
//      val newFirstFrame = _versions(firstFrame)
//      if(newFirstFrame.isReadyForReevaluation) {
//        NotificationResultAction.NotificationOutAndSuccessorOperation.NextReevaluation(finalizedVersion.out, newFirstFrame.txn)
//      } else {
//        NotificationResultAction.NotificationOutAndSuccessorOperation.FollowFraming(finalizedVersion.out, newFirstFrame.txn)
//      }
//    } else {
//      NotificationResultAction.NotificationOutAndSuccessorOperation.NoSuccessor(finalizedVersion.out)
//    }
//    res
//  }
//
//  // =================== READ OPERATIONS ====================
//
//  /**
//    * ensures at least a read version is stored to track executed reads or dynamic operations.
//    * @param txn the executing transaction
//    * @return the version's position.
//    */
//  private def ensureReadVersion(txn: T, knownOrderedMinPos: Int = latestGChint + 1): Int = {
//    assert(knownOrderedMinPos > latestGChint, s"nonsensical minpos $knownOrderedMinPos <= latestGChint $latestGChint")
//    if(knownOrderedMinPos == size) {
//      assert(txn.isTransitivePredecessor(_versions(knownOrderedMinPos - 1).txn) || _versions(knownOrderedMinPos - 1).txn.phase == TurnPhase.Completed, s"illegal $knownOrderedMinPos: predecessor ${_versions(knownOrderedMinPos - 1).txn} not ordered in $this")
//      arrangeVersionArrayAndCreateVersion(size, txn)
//    } else if (_versions(latestKnownStable).txn == txn) {
//      lastGCcount = 0
//      latestKnownStable
//    } else {
//      val (insertOrFound, _) = findOrPigeonHolePropagatingPredictive(txn, knownOrderedMinPos, fromFinalPredecessorRelationIsRecorded = true, size, toFinalRelationIsRecorded = true, UnlockedUnknown)
//      if(insertOrFound < 0) {
//        arrangeVersionArrayAndCreateVersion(-insertOrFound, txn)
//      } else {
//        lastGCcount = 0
//        insertOrFound
//      }
//    }
//  }
//
//  /**
//    * entry point for before(this); may suspend.
//    *
//    * @param txn the executing transaction
//    * @return the corresponding [[Version.value]] from before this transaction, i.e., ignoring the transaction's
//    *         own writes.
//    */
//  def dynamicBefore(txn: T): V = {
//    //    assert(!valuePersistency.isTransient, s"$txn invoked dynamicBefore on transient node")
//    val version = synchronized {
//      val pos = ensureReadVersion(txn)
//      // DO NOT INLINE THIS! it breaks the code! see https://scastie.scala-lang.org/briJDRO3RCmIMEd1zApmBQ
//      _versions(pos)
//    }
//    if(!version.isStable) version.blockForStable()
//    version.lastWrittenPredecessorIfStable.value.get
//  }
//
//  def staticBefore(txn: T): V = {
//    //    assert(!valuePersistency.isTransient, s"$txn invoked staticBefore on transient struct")
//    val version = synchronized {
//      val pos = findFinalPosition(txn)
//      _versions(if (pos < 0) -pos - 1 else pos)
//    }
//    if(version.txn != txn && version.value.isDefined) {
//      version.value.get
//    } else {
//      version.lastWrittenPredecessorIfStable.value.get
//    }
//  }
//
//  /**
//    * entry point for after(this); may suspend.
//    * @param txn the executing transaction
//    * @return the corresponding [[Version.value]] from after this transaction, i.e., awaiting and returning the
//    *         transaction's own write if one has occurred or will occur.
//    */
//  def dynamicAfter(txn: T): V = {
//    val version = synchronized {
//      val pos = ensureReadVersion(txn)
//      // DO NOT INLINE THIS! it breaks the code! see https://scastie.scala-lang.org/briJDRO3RCmIMEd1zApmBQ
//      _versions(pos)
//    }
//    if(!version.isStable) version.blockForStable()
//    if (version.value.isDefined) {
//      version.value.get
//    } else {
//      valuePersistency.unchange.unchange(version.lastWrittenPredecessorIfStable.value.get)
//    }
//  }
//
//  def staticAfter(txn: T): V = {
//    val version = synchronized {
//      val pos = findFinalPosition(txn)
//      _versions(if (pos < 0) -pos - 1 else pos)
//    }
//    if(version.value.isDefined) {
//      if(version.txn == txn) {
//        version.value.get
//      } else {
//        valuePersistency.unchange.unchange(version.value.get)
//      }
//    } else {
//      valuePersistency.unchange.unchange(version.lastWrittenPredecessorIfStable.value.get)
//    }
//  }
//
//  // =================== DYNAMIC OPERATIONS ====================
//
//  /**
//    * entry point for discover(this, add). May suspend.
//    * @param txn the executing reevaluation's transaction
//    * @param add the new edge's sink node
//    * @return the appropriate [[Version.value]].
//    */
//  def discover(txn: T, add: OutDep): (Seq[T], Option[T]) = synchronized {
//    val position = ensureReadVersion(txn)
//    assert(!_versions(position).out.contains(add), "must not discover an already existing edge!")
//    retrofitSourceOuts(position, add, +1)
//  }
//
//  /**
//    * entry point for drop(this, ticket.issuer); may suspend temporarily.
//    * @param txn the executing reevaluation's transaction
//    * @param remove the removed edge's sink node
//    */
//  def drop(txn: T, remove: OutDep): (Seq[T], Option[T]) = synchronized {
//    val position = ensureReadVersion(txn)
//    assert(_versions(position).out.contains(remove), "must not drop a non-existing edge!")
//    retrofitSourceOuts(position, remove, -1)
//  }
//
//  /**
//    * performs the reframings on the sink of a discover(n, this) with arity +1, or drop(n, this) with arity -1
//    * @param successorWrittenVersions the reframings to perform for successor written versions
//    * @param maybeSuccessorFrame maybe a reframing to perform for the first successor frame
//    * @param arity +1 for discover adding frames, -1 for drop removing frames.
//    */
//  def retrofitSinkFrames(successorWrittenVersions: Seq[T], maybeSuccessorFrame: Option[T], arity: Int): Unit = synchronized {
//    require(math.abs(arity) == 1)
//    var minPos = firstFrame
//    for(txn <- successorWrittenVersions) {
//      val position = ensureReadVersion(txn, minPos)
//      val version = _versions(position)
//      // note: if drop retrofitting overtook a change notification, changed may update from 0 to -1 here!
//      version.changed += arity
//      minPos = position + 1
//    }
//
//    if (maybeSuccessorFrame.isDefined) {
//      val txn = maybeSuccessorFrame.get
//      val position = ensureReadVersion(txn, minPos)
//      val version = _versions(position)
//      // note: conversely, if a (no)change notification overtook discovery retrofitting, pending may change
//      // from -1 to 0 here. No handling is required for this case, because firstFrame < position is an active
//      // reevaluation (the one that's executing the discovery) and will afterwards progressToNextWrite, thereby
//      // executing this then-ready reevaluation, but for now the version is guaranteed not stable yet.
//      version.pending += arity
//    }
//    // cannot make this assertion here because dynamic events might make the firstFrame not a frame when dropping the only incoming changed dependency..
//    //assertOptimizationsIntegrity(s"retrofitSinkFrames(writes=$successorWrittenVersions, maybeFrame=$maybeSuccessorFrame)")
//  }
//
//  /**
//    * rewrites all affected [[Version.out]] of the source this during drop(this, delta) with arity -1 or
//    * discover(this, delta) with arity +1, and collects transactions for retrofitting frames on the sink node
//    * @param position the executing transaction's version's position
//    * @param delta the outgoing dependency to add/remove
//    * @param arity +1 to add, -1 to remove delta to each [[Version.out]]
//    * @return a list of transactions with written successor versions and maybe the transaction of the first successor
//    *         frame if it exists, for which reframings have to be performed at the sink.
//    */
//  private def retrofitSourceOuts(position: Int, delta: OutDep, arity: Int): (Seq[T], Option[T]) = {
//    require(math.abs(arity) == 1)
//    // allocate array to the maximum number of written versions that might follow
//    // (any version at index firstFrame or later can only be a frame, not written)
//    val sizePrediction = math.max(firstFrame - position, 0)
//    val successorWrittenVersions = new ArrayBuffer[T](sizePrediction)
//    var maybeSuccessorFrame: Option[T] = None
//    for(pos <- position until size) {
//      val version = _versions(pos)
//      if(arity < 0) version.out -= delta else version.out += delta
//      // as per above, this is implied false if pos >= firstFrame:
//      if(maybeSuccessorFrame.isEmpty) {
//        if(version.isWritten){
//          successorWrittenVersions += version.txn
//        } else if (version.isFrame) {
//          maybeSuccessorFrame = Some(version.txn)
//        }
//      }
//    }
//    if(successorWrittenVersions.size > sizePrediction) System.err.println(s"FullMV retrofitSourceOuts predicted size max($firstFrame - $position, 0) = $sizePrediction, but size eventually was ${successorWrittenVersions.size}")
//    assertOptimizationsIntegrity(s"retrofitSourceOuts(from=$position, outdiff=$arity $delta) -> (writes=$successorWrittenVersions, maybeFrame=$maybeSuccessorFrame)")
//    (successorWrittenVersions, maybeSuccessorFrame)
//  }
//
//  def fullGC(): Int = synchronized {
//    moveGCHintToLatestCompleted()
//    gcAndLeaveHoles(_versions, _versions(latestGChint).value.isDefined, 0, -1, -1)
//    lastGCcount
//  }
//
//  private def moveGCHintToLatestCompleted(): Unit = {
//    @tailrec @inline def findLastCompleted(to: Int): Unit = {
//      // gc = 0 = completed
//      // to = 1 = !completed
//      if (to > latestGChint) {
//        val idx = latestGChint + (to - latestGChint + 1) / 2
//        // 0 + (1 - 0 + 1) / 2 = 1
//        if (_versions(idx).txn.phase == TurnPhase.Completed) {
//          latestGChint = idx
//          findLastCompleted(to)
//        } else {
//          findLastCompleted(idx - 1)
//        }
//      }
//    }
//
//    val latestPossibleGCHint = firstFrame - 1
//    if (_versions(latestPossibleGCHint).txn.phase == TurnPhase.Completed) {
//      // common case shortcut and corner case: all transactions that can be completed are completed (e.g., graph is in resting state)
//      latestGChint = latestPossibleGCHint
//    } else {
//      findLastCompleted(firstFrame - 2)
//    }
//  }
//
//  private def arrangeVersionArrayAndCreateVersions(insertOne: Int, one: T, insertTwo: Int, two: T): (Int, Int) = {
//    arrangeVersionArray(2, insertOne, insertTwo)
//    val first = insertOne - lastGCcount
//    val second = insertTwo - lastGCcount + 1
//    if(first == size) {
//      val predVersion = _versions(size - 1)
//      val out = predVersion.out
//      val lastWrittenPredecessorIfStable = computeSuccessorWrittenPredecessorIfStable(predVersion)
//      _versions(first) = new Version(one, lastWrittenPredecessorIfStable, out, pending = 0, changed = 0, value = None)
//      _versions(second) = new Version(two, lastWrittenPredecessorIfStable, out, pending = 0, changed = 0, value = None)
//      if(lastWrittenPredecessorIfStable != null) firstFrame += 2
//      size += 2
//      assertOptimizationsIntegrity(s"arrangeVersionsAppend($insertOne -> $first, $one, $insertTwo -> $second, $two)")
//      (first, second)
//    } else {
//      createVersionInHole(first, one)
//      createVersionInHole(second, two)
//      assertOptimizationsIntegrity(s"arrangeVersions($insertOne -> $first, $one, $insertTwo -> $second, $two)")
//      (first, second)
//    }
//  }
//  private def arrangeVersionArrayAndCreateVersion(insertPos: Int, txn: T): Int = {
//    arrangeVersionArray(1, insertPos, -1)
//    val pos = insertPos - lastGCcount
//    createVersionInHole(pos, txn)
//    assertOptimizationsIntegrity(s"arrangeVersions($insertPos -> $pos, $txn)")
//    pos
//  }
//
//  private def arrangeVersionArray(create: Int, firstHole: Int, secondHole: Int): Unit = {
//    assert(create != 0 || (firstHole < 0 && secondHole < 0), s"holes $firstHole and $secondHole do not match 0 insertions")
//    assert(create != 1 || (firstHole >= 0 && secondHole < 0), s"holes $firstHole and $secondHole do not match 1 insertions")
//    assert(create != 2 || (firstHole >= 0 && secondHole >= 0), s"holes $firstHole and $secondHole do not match 2 insertions")
//    assert(secondHole < 0 || secondHole >= firstHole, s"second hole ${secondHole }must be behind or at first $firstHole")
//    if(firstHole == size && size + create <= _versions.length) {
//      // if only versions should be added at the end (i.e., existing versions don't need to be moved) and there's enough room, just don't do anything
//      lastGCcount = 0
//    } else {
//      if (NodeVersionHistory.DEBUG_GC) println(s"[${Thread.currentThread().getName}] gc attempt to insert $create: ($firstHole, $secondHole) in $this")
//      val hintVersionIsWritten = _versions(latestGChint).value.isDefined
//      val straightDump = latestGChint - (if (hintVersionIsWritten) 0 else 1)
//      if(straightDump == 0 && size + create <= _versions.length) {
//        if (NodeVersionHistory.DEBUG_GC) println(s"[${Thread.currentThread().getName}] hintgc($latestGChint): -$straightDump would have no effect, but history rearrangement is possible")
//        arrangeHolesWithoutGC(_versions, firstHole, secondHole)
//      } else if (size - straightDump + create <= _versions.length) {
//        if(NodeVersionHistory.DEBUG_GC) println(s"[${Thread.currentThread().getName}] hintgc($latestGChint): -$straightDump accepted")
//        gcAndLeaveHoles(_versions, hintVersionIsWritten, create, firstHole, secondHole)
//      } else {
//        // straight dump with gc hint isn't enough: see what full GC brings
//        if(NodeVersionHistory.DEBUG_GC) println(s"[${Thread.currentThread().getName}] hintgc($latestGChint): -$straightDump insufficient and not enough room for history rearrangement")
//        moveGCHintToLatestCompleted()
//        val fullGCVersionIsWritten = _versions(latestGChint).value.isDefined
//        val fullDump = latestGChint - (if (fullGCVersionIsWritten) 0 else 1)
//        if (size - fullDump + create <= _versions.length) {
//          if(NodeVersionHistory.DEBUG_GC) println(s"[${Thread.currentThread().getName}] fullgc($latestGChint): -$fullDump accepted")
//          gcAndLeaveHoles(_versions, fullGCVersionIsWritten, create, firstHole, secondHole)
//        } else {
//          // full GC also isn't enough either: grow the array.
//          val grown = new Array[Version](_versions.length + (_versions.length >> 1))
//          if(fullDump == 0) {
//            if(NodeVersionHistory.DEBUG_GC) println(s"[${Thread.currentThread().getName}] fullgc($latestGChint): -$fullDump would have no effect, rearraging after growing max size ${_versions.length} -> ${grown.length}")
//            if(firstHole > 0) System.arraycopy(_versions, 0, grown, 0, firstHole)
//            arrangeHolesWithoutGC(grown, firstHole, secondHole)
//          } else {
//            if(NodeVersionHistory.DEBUG_GC) println(s"[${Thread.currentThread().getName}] fullgc($latestGChint): -$fullDump insufficient, also growing max size ${_versions.length} -> ${grown.length}")
//            gcAndLeaveHoles(grown, fullGCVersionIsWritten, create, firstHole, secondHole)
//          }
//          _versions = grown
//        }
//      }
//      if(NodeVersionHistory.DEBUG_GC) println(s"[${Thread.currentThread().getName}] after gc of $lastGCcount, holes at (${if(firstHole == -1) -1 else firstHole - lastGCcount}, ${if(secondHole == -1) -1 else secondHole - lastGCcount + 1}): $this")
//    }
//  }
//
//  private def arrangeHolesWithoutGC(writeTo: Array[Version], firstHole: Int, secondHole: Int): Unit = {
//    if (firstHole >= 0 && firstHole < size) {
//      if (secondHole < 0 || secondHole == size) {
//        System.arraycopy(_versions, firstHole, writeTo, firstHole + 1, size - firstHole)
//      } else {
//        System.arraycopy(_versions, secondHole, writeTo, secondHole + 2, size - secondHole)
//        if (secondHole != firstHole) System.arraycopy(_versions, firstHole, writeTo, firstHole + 1, secondHole - firstHole)
//      }
//    }
//    lastGCcount = 0
//  }
//
//  private def gcAndLeaveHoles(writeTo: Array[Version], hintVersionIsWritten: Boolean, create: Int, firstHole: Int, secondHole: Int): Unit = {
//    // if a straight dump using the gc hint makes enough room, just do that
//    if (hintVersionIsWritten) {
//      if(NodeVersionHistory.DEBUG_GC) println(s"[${Thread.currentThread().getName}] hint is written: dumping $latestGChint to offset 0")
//      // if hint is written, just dump everything before
//      latestKnownStable -= latestGChint
//      dumpToOffsetAndLeaveHoles(writeTo, latestGChint, 0, firstHole, secondHole)
//      lastGCcount = latestGChint
//    } else {
//      // otherwise find the latest write before the hint, move it to index 0, and only dump everything else
//      lastGCcount = latestGChint - 1
//      writeTo(0) = _versions(latestGChint).lastWrittenPredecessorIfStable
//      latestKnownStable = if(latestKnownStable <= latestGChint) 0 else latestKnownStable - lastGCcount
//      dumpToOffsetAndLeaveHoles(writeTo, latestGChint, 1, firstHole, secondHole)
//    }
//    writeTo(0).lastWrittenPredecessorIfStable = null
//    val sizeBefore = size
//    latestGChint -= lastGCcount
//    firstFrame -= lastGCcount
//    size -= lastGCcount
//    if ((_versions eq writeTo) && size + create < sizeBefore) java.util.Arrays.fill(_versions.asInstanceOf[Array[AnyRef]], size + create, sizeBefore, null)
//  }
//
//  private def dumpToOffsetAndLeaveHoles(writeTo: Array[Version], retainFrom: Int, retainTo: Int, firstHole: Int, secondHole: Int): Unit = {
//    assert(retainFrom > retainTo, s"this method is either broken or pointless (depending on the number of inserts) if not at least one version is removed.")
//    assert(firstHole >= 0 || secondHole < 0, "must not give only a second hole")
//    assert(secondHole < 0 || secondHole >= firstHole, "second hole must be behind or at first")
//    // just dump everything before the hint
//    if (firstHole < 0 || firstHole == size) {
//      // no hole or holes at the end only: the entire array stays in one segment
//      System.arraycopy(_versions, retainFrom, writeTo, retainTo, size - retainFrom)
//    } else {
//      // copy first segment
//      System.arraycopy(_versions, retainFrom, writeTo, retainTo, firstHole - retainFrom)
//      val gcOffset = retainTo - retainFrom
//      val newFirstHole = gcOffset + firstHole
//      if (secondHole < 0 || secondHole == size) {
//        // no second hole or second hole at the end only: there are only two segments
//        if((_versions ne writeTo) || gcOffset != 1) System.arraycopy(_versions, firstHole, writeTo, newFirstHole + 1, size - firstHole)
//      } else {
//        if (secondHole != firstHole && ((_versions ne writeTo) || gcOffset != 1)) System.arraycopy(_versions, firstHole, writeTo, newFirstHole + 1, secondHole - firstHole)
//        if((_versions ne writeTo) || gcOffset != 2) System.arraycopy(_versions, secondHole, writeTo, gcOffset + secondHole + 2, size - secondHole)
//      }
//    }
//  }
//}
