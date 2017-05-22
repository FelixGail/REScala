package rescala.fullmv

import rescala.engine.ValuePersistency

import scala.annotation.elidable.ASSERTION
import scala.annotation.{elidable, tailrec}
import scala.collection.mutable.ArrayBuffer

class Version[D, T, R](val txn: T, var stable: Boolean, var out: Set[R], var pending: Int, var changed: Int, var value: Option[D]) {
  def isWritten: Boolean = changed == 0 && value.isDefined
  def isReadOrDynamic: Boolean = pending == 0 && changed == 0 && value.isEmpty
  def isOvertakeCompensation: Boolean = pending < 0 || changed < 0
  def isFrame: Boolean = pending > 0 || changed > 0
  def isWrittenOrFrame: Boolean = isWritten || isFrame
  def isReadyForReevaluation: Boolean = pending == 0 && changed > 0
  def read(): D = {
    assert(isWritten, "reading un-written "+this)
    value.get
  }

  override def toString: String = {
    if(isWritten){
      "Written(" + txn + ", out=" + out + ", v=" + value.get + ")"
    } else if (isReadOrDynamic) {
      (if(stable) "Stable" else "Unstable") + "Marker(" + txn + ", out=" + out + ")"
    } else if (isOvertakeCompensation) {
      "OvertakeCompensation(" + txn + ", " + (if (stable) "stable" else "unstable") + ", out=" + out + ", pending=" + pending + ", changed=" + changed + ")"
    } else if(isFrame) {
      if(stable) {
        if(isReadyForReevaluation) {
          "Active(" + txn + ", out=" + out + ")"
        } else {
          "FirstFrame(" + txn + ", out=" + out + ", pending=" + pending + ", changed=" + changed + ")"
        }
      } else {
        if(isReadyForReevaluation) {
          "Queued(" + txn + ", out=" + out + ")"
        } else {
          "Frame(" + txn + ", out=" + out + ", pending=" + pending + ", changed=" + changed + ")"
        }
      }
    } else {
      "UnknownVersionCase!(" + txn + ", " + (if(stable) "stable" else "unstable") + ", out=" + out + ", pending=" + pending + ", changed=" + changed + ", value = " + value + ")"
    }
  }
}

sealed trait FramingBranchResult[+T, +R]
object FramingBranchResult {
  case object FramingBranchEnd extends FramingBranchResult[Nothing, Nothing]
  case class FramingBranchOut[R](out: Set[R]) extends FramingBranchResult[Nothing, R]
  case class FramingBranchOutSuperseding[T, R](out: Set[R], supersede: T) extends FramingBranchResult[T, R]
}

sealed trait NotificationResultAction[+T, +R]
object NotificationResultAction {
  case object NotGlitchFreeReady extends NotificationResultAction[Nothing, Nothing]
  case object ResolvedNonFirstFrameToUnchanged extends NotificationResultAction[Nothing, Nothing]
  case object GlitchFreeReadyButQueued extends NotificationResultAction[Nothing, Nothing]
  case object GlitchFreeReady extends NotificationResultAction[Nothing, Nothing]
  sealed trait NotificationOutAndSuccessorOperation[+T, R] extends NotificationResultAction[T, R] {
    val out: Set[R]
  }
  object NotificationOutAndSuccessorOperation {
    case class NoSuccessor[R](out: Set[R]) extends NotificationOutAndSuccessorOperation[Nothing, R]
    case class FollowFraming[T, R](out: Set[R], succTxn: T) extends NotificationOutAndSuccessorOperation[T, R]
    case class NextReevaluation[T, R](out: Set[R], succTxn: T) extends NotificationOutAndSuccessorOperation[T, R]
  }
}

/**
  * A node version history datastructure
  * @param sgt the transaction ordering authority
  * @param init the initial creating transaction
  * @param valuePersistency the value persistency descriptor
  * @tparam V the type of stored values
  * @tparam T the type of transactions
  * @tparam R the type of dependency nodes (reactives)
  */
class NodeVersionHistory[V, T, R](val sgt: SerializationGraphTracking[T], init: T, val valuePersistency: ValuePersistency[V]) {
  @elidable(ASSERTION) @inline
  def assertOptimizationsIntegrity(debugOutputDescription: => String): Unit = {
    def debugStatement(whatsWrong: String): String = s"$debugOutputDescription left $whatsWrong (latestWritten $latestWritten, ${if(firstFrame > _versions.size) "no frames" else "firstFrame "+firstFrame}): \n  " + _versions.zipWithIndex.map{case (version, index) => s"$index: $version"}.mkString("\n  ")

    assert(firstFrame >= _versions.size || _versions(firstFrame).isFrame, debugStatement("firstFrame not frame"))
    assert(!_versions.take(firstFrame).exists(_.isFrame), debugStatement("firstFrame not first"))

    assert(_versions(latestWritten).isWritten, debugStatement("latestWritten not written"))
    assert(!_versions.drop(latestWritten + 1).exists(_.isWritten), debugStatement("latestWritten not latest"))

    assert(!_versions.zipWithIndex.exists{case (version, index) => version.stable != (index <= firstFrame)}, debugStatement("broken version stability"))
  }

  // =================== STORAGE ====================

  var _versions = new ArrayBuffer[Version[V, T, R]](6)
  _versions += new Version[V, T, R](init, stable = true, out = Set(), pending = 0, changed = 0, Some(valuePersistency.initialValue))
  var latestValue: V = valuePersistency.initialValue

  var incomings: Set[R] = Set.empty

  /**
    * creates a version and inserts it at the given position; default parameters create a "read" version
    * @param position the position
    * @param txn the transaction this version is associated with
    * @param pending the number of pending notifications, none by default
    * @param changed the number of already received change notifications, none by default
    * @return the created version
    */
  private def createVersion(position: Int, txn: T, pending: Int = 0, changed: Int = 0): Version[V, T, R] = {
    assert(position > 0, "cannot create a version at negative position")
    val version = new Version[V, T, R](txn, stable = position <= firstFrame, _versions(position - 1).out, pending, changed, None)
    _versions.insert(position, version)
    if(position <= firstFrame) firstFrame += 1
    if(position <= latestWritten) latestWritten += 1
    version
  }


  // =================== NAVIGATION ====================
  var firstFrame: Int = _versions.size
  var latestWritten: Int = 0

  /**
    * performs binary search for the given transaction in _versions. If no version associated with the given
    * transaction is found, it establishes an order in sgt against all other versions' transactions, placing
    * the given transaction as late as possible. For this it first finds the latest possible insertion point. Assume,
    * for a given txn, this is between versions y and z. As the latest point is used, it is txn < z.txn. To fixate this
    * order, the search thus attempts to establish y.txn < txn in sgt. This may fail, however, if concurrent
    * threads intervened and established txn < y.txn. For this case, the search keeps track of the latest x for which
    * x.txn < txn is already established. It then restarts the search in the reduced fallback range between x and y.
    * @param lookFor the transaction to look for
    * @param recoverFrom current fallback from index in case of order establishment fallback
    * @param from current from index of the search range
    * @param to current to index (exclusive) of the search range
    * @return the index of the version associated with the given transaction, or if no such version exists the negative
    *         index it would have to be inserted. Note that we assume no insertion to ever occur at index 0 -- the
    *         the first version of any node is either from the transaction that created it or from some completed
    *         transaction. In the former case, no preceding transaction should be aware of the node's existence. In the
    *         latter case, there are no preceding transactions that still execute operations. Thus insertion at index 0
    *         should be impossible. A return value of 0 thus means "found at 0", rather than "should insert at 0"
    */
  @tailrec
  private def findOrPidgeonHole(lookFor: T, recoverFrom: Int, from: Int, to: Int): Int = {
    if (to == from) {
      assert(from > 0, s"Found an insertion point of 0 for $lookFor; insertion points must always be after the base state version.")
      if(sgt.ensureOrder(_versions(from - 1).txn, lookFor) == FirstFirst) {
        -from
      } else {
        assert(recoverFrom < from, s"Illegal position hint: $lookFor must be before $recoverFrom")
        findOrPidgeonHole(lookFor, recoverFrom, recoverFrom, from)
      }
    } else {
      val idx = from+(to-from-1)/2
      val candidate = _versions(idx).txn
      if(candidate == lookFor) {
        idx
      } else sgt.getOrder(candidate, lookFor) match {
        case FirstFirst =>
          findOrPidgeonHole(lookFor, idx + 1, idx + 1, to)
        case SecondFirst =>
          findOrPidgeonHole(lookFor, recoverFrom, from, idx)
        case Unordered =>
          findOrPidgeonHole(lookFor, recoverFrom, idx + 1, to)
      }
    }
  }

  /**
    * determine the position or insertion point for a framing transaction
    *
    * @param txn the transaction
    * @return the position (positive values) or insertion point (negative values)
    */
  private def findFrame(txn: T): Int = {
    findOrPidgeonHole(txn, latestWritten, latestWritten, _versions.size)
  }

  /**
    * traverse history back in time from the given position until the first write (i.e. [[Version.isWrittenOrFrame]])
    * @param txn the transaction for which the previous write is being searched; used for error reporting only.
    * @param from the transaction's position, search starts at this position's predecessor version and moves backwards
    * @return the first encountered version with [[Version.isWrittenOrFrame]]
    */
  private def prevWrite(txn: T, from: Int): Version[V, T, R] = {
    var position = from - 1
    while(position >= 0 && !_versions(position).isWrittenOrFrame) position -= 1
    if(position < 0) throw new IllegalArgumentException(txn + " does not have a preceding write in versions " + _versions.take(from))
    _versions(position)
  }

  // =================== FRAMING ====================

  /**
    * entry point for superseding framing
    * @param txn the transaction visiting the node for framing
    * @param supersede the transaction whose frame was superseded by the visiting transaction at the previous node
    */
  def incrementSupersedeFrame(txn: T, supersede: T): FramingBranchResult[T, R] = synchronized {
    val result = incrementFrame0(txn)
    val supersedePosition = findFrame(supersede)
    if (supersedePosition >= 0) {
      _versions(supersedePosition).pending -= 1
    } else {
      createVersion(-supersedePosition, supersede, pending = -1)
    }
    assertOptimizationsIntegrity(s"incrementSupersedeFrame($txn, $supersede) -> $result")
    result
  }

  /**
    * entry point for regular framing
    * @param txn the transaction visiting the node for framing
    */
  def incrementFrame(txn: T): FramingBranchResult[T, R] = synchronized {
    val result = incrementFrame0(txn)
    assertOptimizationsIntegrity(s"incrementFrame($txn) -> $result")
    result
  }

  /**
    * creates a frame if none exists, or increments an existing frame's [[Version.pending]] counter.
    * @param txn the transaction visiting the node for framing
    * @return a descriptor of how this framing has to propagate
    */
  private def incrementFrame0(txn: T): FramingBranchResult[T, R] = {
    val position = findFrame(txn)
    if(position >= 0) {
      val version = _versions(position)
      version.pending += 1
      if(version.pending == 1 && position < firstFrame) {
        // this case (a version is framed for the "first" time, i.e., pending == 1, during framing, but already existed
        // beforehand) is relevant if drop retrofitting downgraded a frame into a marker and the marker and subsequent
        // versions were subsequently stabilized. Since this marker is now upgraded back into a frame, subsequent
        // versions need to be unstabilized again. This is safe to do because this method is only called by framing
        // transactions, meaning all subsequent versions must also be frames, deleted frame markers, or to be deleted
        // frame drop compensations. This case cannot occur for follow framings attached to change notifications,
        // because these imply a preceding reevaluation, meaning neither the incremented frame nor any subsequent
        // frame can be stable yet.
        firstFrame = position
        var pos = position + 1
        while(pos < _versions.size && _versions(pos).stable) {
          _versions(pos).stable = false
          pos += 1
        }
      }
      FramingBranchResult.FramingBranchEnd
    } else {
      val frame = createVersion(-position, txn, pending = 1)
      if(-position < firstFrame) {
        val previousFirstFrame = firstFrame
        firstFrame = -position
        if(previousFirstFrame < _versions.size) {
          val supersede = _versions(previousFirstFrame)
          supersede.stable = false
          FramingBranchResult.FramingBranchOutSuperseding(frame.out, supersede.txn)
        } else {
          FramingBranchResult.FramingBranchOut(frame.out)
        }
      } else {
        FramingBranchResult.FramingBranchEnd
      }
    }
  }

  /*
   * =================== NOTIFICATIONS/ / REEVALUATION ====================
   */

  /**
    * entry point for change/nochange notification reception
    * @param txn the transaction sending the notification
    * @param changed whether or not the dependency changed
    * @param maybeFollowFrame possibly a transaction for which to create a subsequent frame, furthering its partial framing.
    */
  def notify(txn: T, changed: Boolean, maybeFollowFrame: Option[T]): NotificationResultAction[T, R] = synchronized {
    val position = findFrame(txn)

    // do follow framing
    if(maybeFollowFrame.isDefined) {
      val followFrameMinPosition = if(position > 0) position + 1 else -position
      val followTxn = maybeFollowFrame.get
      // because we know that txn << followTxn, followTxn cannot be involved with firstFrame stuff.
      // thus followTxn also does not need this framing propagated by itself.
      val followPosition = findOrPidgeonHole(followTxn, followFrameMinPosition, followFrameMinPosition, _versions.size)
      if(followPosition >= 0) {
        _versions(followPosition).pending += 1
      } else {
        createVersion(-followPosition, followTxn, pending = 1)
      }
    }

    // apply notification changes
    val result = if(position < 0) {
      assert(-position != firstFrame, "(no)change notification, which overtook (a) discovery retrofitting or " +
        "(b) predecessor (no)change notification with followFraming for this transaction, wants to create FirstFrame, " +
        "which should be impossible because firstFrame should be (a) the predecessor reevaluation performing said retrofitting or " +
        "(b) the predecessor reevaluation pending reception of said (no)change notification.")
      // note: this case occurs if a (no)change notification overtook discovery retrofitting, and sets pending to -1!
      // In this case, we simply return the results as if discovery retrofitting had already happened: As we know
      // that it is still in progress, there must be a preceding active reevaluation (hence above assertion), so the
      // retrofitting would simply create a queued frame, which this notification would either:
      if(changed) {
        // convert into a queued reevaluation
        createVersion(-position, txn, pending = -1, changed = 1)
        NotificationResultAction.GlitchFreeReadyButQueued
      } else {
        // or resolve into an unchanged marker
        createVersion(-position, txn, pending = -1)
        NotificationResultAction.ResolvedNonFirstFrameToUnchanged
      }
    } else {
      val version = _versions(position)
      // This assertion is probably pointless as it only verifies a subset of assertStabilityIsCorrect, i.e., if this
      // would fail, then assertStabilityIsCorrect will have failed at the end of the previous operation already.
      assert((position == firstFrame) == version.stable, "firstFrame and stable diverted..")

      // note: if the notification overtook a previous turn's notification with followFraming for this transaction,
      // pending may update from 0 to -1 here
      version.pending -= 1
      if (changed) {
        // note: if drop retrofitting overtook the change notification, change may update from -1 to 0 here!
        version.changed += 1
      }

      // check if the notification triggers subsequent actions
      if (version.pending == 0) {
        if (position == firstFrame) {
          if (version.changed > 0) {
            NotificationResultAction.GlitchFreeReady
          } else {
            // ResolvedFirstFrameToUnchanged
            progressToNextWriteForNotification(s"$this notify ResolvedFirstFrame($position)ToUnchanged", version.out)
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
    assertOptimizationsIntegrity(s"notify($txn, $changed, $maybeFollowFrame) -> $result")
    result
  }

  def reevIn(turn: T): (V, Set[R]) = {
    val firstFrameTurn = synchronized {_versions(firstFrame).txn}
    assert(firstFrameTurn == turn, s"Turn $turn called reevIn, but Turn $firstFrameTurn is first frame owner")
    (latestValue, incomings)
  }

  /**
    * progress [[firstFrame]] forward until a [[Version.isWrittenOrFrame]] is encountered, and
    * return the resulting notification out (with reframing if subsequent write is found).
    */
  def reevOut(turn: T, maybeValue: Option[V]): NotificationResultAction.NotificationOutAndSuccessorOperation[T, R] = synchronized {
    val position = firstFrame
    val version = _versions(position)
    assert(version.txn == turn, s"$turn called reevDone, but first frame is $version (different transaction)")
    assert(version.value.isEmpty, s"cannot write twice: $version")
    assert(!version.isOvertakeCompensation && (version.isReadyForReevaluation || (version.isReadOrDynamic && maybeValue.isEmpty)), s"cannot write $version")

    if(maybeValue.isDefined) {
      latestWritten = position
      this.latestValue = maybeValue.get
      version.value = maybeValue
    }
    version.changed = 0

    val result = progressToNextWriteForNotification(s"$this ReevOut($position)${if(maybeValue.isDefined) "Changed" else "Unchanged"}", version.out)
    assertOptimizationsIntegrity(s"reevOut($turn, ${maybeValue.isDefined}) -> $result")
    result
  }

  /**
    * progresses [[firstFrame]] forward until a [[Version.isWrittenOrFrame]] is encountered (which is implied not
    * [[Version.isWritten]] due to being positioned at/behind [[firstFrame]]) and assemble all necessary
    * information to send out change/nochange notifications for the given transaction. Also capture synchronized,
    * whether or not the possibly encountered write [[Version.isReadyForReevaluation]].
    * @return the notification and next reevaluation descriptor.
    */
  private def progressToNextWriteForNotification(debugOutputString: => String, out: Set[R]): NotificationResultAction.NotificationOutAndSuccessorOperation[T, R] = {
    @tailrec
    def progressToNextWriteForNotification0(): NotificationResultAction.NotificationOutAndSuccessorOperation[T, R] = {
      firstFrame += 1
      if (firstFrame < _versions.size) {
        val version = _versions(firstFrame)

        assert(!version.stable, s"$debugOutputString cannot stabilize already-stable $version at position $firstFrame")
        version.stable = true

        if (version.isWrittenOrFrame) {
          if (version.isReadyForReevaluation) {
            NotificationResultAction.NotificationOutAndSuccessorOperation.NextReevaluation(out, version.txn)
          } else {
            NotificationResultAction.NotificationOutAndSuccessorOperation.FollowFraming(out, version.txn)
          }
        } else {
          progressToNextWriteForNotification0()
        }
      } else {
        NotificationResultAction.NotificationOutAndSuccessorOperation.NoSuccessor(out)
      }
    }
    val res = progressToNextWriteForNotification0()
    notifyAll()
    res
  }

  // =================== READ OPERATIONS ====================

  /**
    * ensures at least a read version is stored to track executed reads or dynamic operations.
    * @param txn the executing transaction
    * @return the version's position.
    */
  private def ensureReadVersion(txn: T, knownMinPos: Int = 0): Int = {
    val position = findOrPidgeonHole(txn, knownMinPos, knownMinPos, _versions.size)
    if(position < 0) {
      createVersion(-position, txn)
      -position
    } else {
      position
    }
  }

  def synchronizeDynamicAccess(txn: T): Int = synchronized {
    val position = ensureReadVersion(txn)
    assertOptimizationsIntegrity(s"ensureReadVersion($txn)")
    val version = _versions(position)
    while(!version.stable) wait()

    val stablePosition = if(version.isFrame) firstFrame else findOrPidgeonHole(txn, 1, 1, firstFrame)
    assert(stablePosition >= 0, "somehow, the version allocated above disappeared..")
    stablePosition
  }

  /**
    * entry point for before(this); may suspend.
    * @param txn the executing transaction
    * @return the corresponding [[Version.value]] from before this transaction, i.e., ignoring the transaction's
    *         own writes.
    */
  def dynamicBefore(txn: T): V = synchronized {
    before(txn, synchronizeDynamicAccess(txn))
  }

  def staticBefore(txn: T): V = synchronized {
    before(txn, math.abs(findOrPidgeonHole(txn, 1, 1, firstFrame)))
  }

  private def before(txn: T, position: Int): V = synchronized {
    assert(!valuePersistency.isTransient, "before read on transient node")
    prevWrite(txn, position).read()
  }

  /**
    * entry point for now(this); may suspend.
    * @param txn the executing transaction
    * @return the corresponding [[Version.value]] at the current point in time (but still serializable), i.e.,
    *         possibly returning the transaction's own write if this already occurred.
    */
  def dynamicNow(txn: T): V = synchronized {
    val position = synchronizeDynamicAccess(txn)
    nowGivenOwnVersion(txn, position)
  }

  def staticNow(txn: T): V = synchronized {
    val position = findOrPidgeonHole(txn, 1, 1, firstFrame)
    if(position < 0) {
      beforeOrInit(txn, -position)
    } else {
      nowGivenOwnVersion(txn, position)
    }
  }

  private def nowGivenOwnVersion(txn: T, position: Int): V = {
    val ownVersion = _versions(position)
    if(ownVersion.isWritten) {
      ownVersion.read()
    } else {
      beforeOrInit(txn, position)
    }
  }

  private def beforeOrInit(txn: T, position: Int): V = synchronized {
    if(valuePersistency.isTransient) valuePersistency.initialValue else before(txn, position)
  }

  /**
    * entry point for after(this); may suspend.
    * @param txn the executing transaction
    * @return the corresponding [[Version.value]] from after this transaction, i.e., awaiting and returning the
    *         transaction's own write if one has occurred or will occur.
    */
  def dynamicAfter(txn: T): V = {
    val position = synchronizeDynamicAccess(txn)
    val version = _versions(position)
    // Note: isWrite, not isWritten!
    if(version.isWrittenOrFrame) {
      while(!version.isWritten) {
        // TODO figure out how we want to do this
        System.err.println(s"WARNING: Suspending "+txn+" for dynamic after on its own placeholder; aside from user-specified deadlocks, this may deadlock the application from depleting the task pool of workers.")
        wait()
      }
      version.read()
    } else {
      beforeOrInit(txn, position)
    }
  }

  def staticAfter(txn: T): V = {
    val position = findOrPidgeonHole(txn, 1, 1, firstFrame)
    if(position < 0) {
      beforeOrInit(txn, -position)
    } else {
      val version = _versions(position)
      // Note: isWrite, not isWritten!
      if(version.isWrittenOrFrame) {
        version.read()
      } else {
        beforeOrInit(txn, position)
      }
    }
  }

  // =================== DYNAMIC OPERATIONS ====================

  /**
    * entry point for discover(this, add). May suspend.
    * @param txn the executing reevaluation's transaction
    * @param add the new edge's sink node
    * @return the appropriate [[Version.value]].
    */
  def discover(txn: T, add: R): (ArrayBuffer[T], Option[T]) = synchronized {
    val position = ensureReadVersion(txn)
    assertOptimizationsIntegrity(s"ensureReadVersion($txn)")
    assert(!_versions(position).out.contains(add), "must not discover an already existing edge!")
    retrofitSourceOuts(position, add, +1)
  }

  /**
    * entry point for drop(this, ticket.issuer); may suspend temporarily.
    * @param txn the executing reevaluation's transaction
    * @param remove the removed edge's sink node
    */
  def drop(txn: T, remove: R): (ArrayBuffer[T], Option[T]) = synchronized {
    val position = ensureReadVersion(txn)
    assertOptimizationsIntegrity(s"ensureReadVersion($txn)")
    assert(_versions(position).out.contains(remove), "must not drop a non-existing edge!")
    retrofitSourceOuts(position, remove, -1)
  }

  /**
    * performs the reframings on the sink of a discover(n, this) with arity +1, or drop(n, this) with arity -1
    * @param successorWrittenVersions the reframings to perform for successor written versions
    * @param maybeSuccessorFrame maybe a reframing to perform for the first successor frame
    * @param arity +1 for discover adding frames, -1 for drop removing frames.
    */
  def retrofitSinkFrames(successorWrittenVersions: ArrayBuffer[T], maybeSuccessorFrame: Option[T], arity: Int): Unit = synchronized {
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
  private def retrofitSourceOuts(position: Int, delta: R, arity: Int): (ArrayBuffer[T], Option[T]) = {
    require(math.abs(arity) == 1)
    // allocate array to the maximum number of written versions that might follow
    // (any version at index firstFrame or later can only be a frame, not written)
    val successorWrittenVersions = new ArrayBuffer[T](firstFrame - position - 1)
    for(pos <- position until _versions.size) {
      val version = _versions(pos)
      if(arity < 0) version.out -= delta else version.out += delta
      // as per above, this is implied false if pos >= firstFrame:
      if(version.isWritten) successorWrittenVersions += version.txn
    }
    val maybeSuccessorFrame = if (firstFrame < _versions.size) Some(_versions(firstFrame).txn) else None
    (successorWrittenVersions, maybeSuccessorFrame)
  }
}
