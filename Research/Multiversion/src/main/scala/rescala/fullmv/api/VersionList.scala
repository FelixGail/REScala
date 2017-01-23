package rescala.fullmv.api

import rescala.fullmv.rescala.fullmv.util.{Result, Recurse, Trampoline}

import scala.annotation.tailrec
import scala.collection.Searching.{Found, InsertionPoint, SearchResult}
import scala.collection.mutable.ArrayBuffer
import scala.collection.{SortedMap, mutable}

class SignalVersionList[V](val host: Host, init: Transaction, initialValue: V, val userComputation: V => V) {
  type O = SignalVersionList[_]
  sealed abstract class Version(val txn: Transaction, var out: Set[O], val performedFraming: Boolean, val performedChangedPropagation: Boolean)
  final class ReadVersion(txn: Transaction, out: Set[O]) extends Version(txn, out, false, false)
  sealed abstract class Frame(txn: Transaction, out: Set[O]) extends Version(txn, out, true, false)
  final class Pending(txn: Transaction, out: Set[O], var pending: Int = 1, var changed: Int = 0) extends Frame(txn, out)
  final class Ready(txn: Transaction, out: Set[O]) extends Frame(txn, out)
  final class Active(txn: Transaction, out: Set[O]) extends Frame(txn, out)
  final class Written(txn: Transaction, out: Set[O], val value: V) extends Version(txn, out, true, true)

  var _versions = new ArrayBuffer[Version](6)
  _versions += new Written(init, Set(), initialValue)

  // =================== NAVIGATION ====================

  private def position(txn: Transaction): SearchResult = {
    ensureOrdered(txn, InsertionPoint(_versions.size))
    @tailrec
    def ensureOrdered(txn: Transaction, attempt: InsertionPoint): SearchResult = {
      if(host.sgt.requireOrder(_versions(attempt.insertionPoint - 1).txn, txn) == FirstFirst) {
        attempt
      } else {
        import scala.collection.Searching._
        _versions.search[Transaction](txn)(host.sgt.fairSearchOrdering) match {
          case found @ Found(_) => found
          case attempt @ InsertionPoint(_) => ensureOrdered(txn, attempt)
          // TODO could optimize this case by not restarting with entire array bounds, but need to track fixedMin and min separately, following both above -1 cases.
        }
      }
    }
  }

  private def prevReevPosition(from: Int): Int = {
    var prevReevPosition = from - 1
    while(prevReevPosition >= 0 && !_versions(prevReevPosition).performedFraming) prevReevPosition -= 1
    if(prevReevPosition < 0) throw new IllegalArgumentException("Does not have a preceding Reevaluation: "+_versions(from))
    prevReevPosition
  }

  private def nextReevPosition(from: Int): Option[Int] = {
    var nextReevPosition = from + 1
    while(nextReevPosition < _versions.size && !_versions(nextReevPosition).performedFraming) nextReevPosition += 1
    if(nextReevPosition < _versions.size) {
      Some(nextReevPosition)
    } else {
      None
    }
  }

  // =================== READ SUSPENSION INFRASTRUCTURE ====================

  private def blockUntilNoLongerFrame(txn: Transaction) = {
    // parameter included to allow parking-per-frame blocking/unblocking
    // but we implement coarse parking-per-node only, so we ignore it here.
    wait()
  }

  private def frameRemoved(txn: Transaction): Unit = {
    // parameter included to allow parking-per-frame blocking/unblocking
    // but we implement coarse parking-per-node only, so we ignore it here.
    notifyAll()
  }

  private def frameDropped(txn: Transaction): Unit = {
    // parameter included to allow parking-per-frame blocking/unblocking
    // but we implement coarse parking-per-node only, so we ignore it here.
    notifyAll()
    // Note that these should ideally be batched; one edge drop may result
    // in multiple frame drops each successive node, only notifying once at
    // the end would suffice for the coarse parking.
  }

  // =================== FRAMING ====================

  def incrementFrame(txn: Transaction): Set[O] = synchronized {
    def createPending(position: Int, out: Set[O]): Pending = {
      val pending = new Pending(txn, out)
      if(_versions(prevReevPosition(position)).performedChangedPropagation) pending.pending += 1
      pending
    }

    position(txn) match {
      case Found(position) =>
        _versions(position) match {
          case frame: Pending =>
            frame.pending += 1
            Set.empty
          case read: ReadVersion =>
            _versions(position) = createPending(position, read.out)
            read.out
          case version =>
            throw new IllegalStateException("Cannot incrementFrame: not a Frame or ReadVersion: " + version)
        }
      case InsertionPoint(position) =>
        val out = _versions(position - 1).out
        _versions.insert(position, createPending(position, out))
        out
    }
  }

  /*
   * =================== CHANGE / NOCHANGE / REEVALUATION ====================
   *
   * Note: This is a big pile of mutually recursive methods.
   * Upon any received change or no-change notification, this stack iterates through the version list
   * and executes all reevaluations that are not missing further notifications from other nodes.
   * The trampoline class is used to enable tail call optimization.
   * For a control flow visualization, consult the following diagram:
   * doc/single node reevaluation pipeline mutual trampoline tail recursion.graphml
   */

  private def notifyFrameRemovedAndCheckForSubsequentReevaluation(position: Int): Trampoline[Option[(Transaction, V)]] = {
    frameRemoved(_versions(position).txn)
    nextReevPosition(position) match {
      case Some(next) => Recurse({ () => unchanged(next)})
      case None => Result(None)
    }
  }

  private def reev_unchanged(position: Int): Trampoline[Option[(Transaction, V)]] = {
    val frame = _versions(position)
    _versions(position) = new ReadVersion(frame.txn, frame.out)
    frame.out.foreach{ succ => host.taskPool.addNoChangeNotification(frame.txn, succ) }
    notifyFrameRemovedAndCheckForSubsequentReevaluation(position)
  }

  private def reev_changed(position: Int, value: V): Trampoline[Option[(Transaction, V)]] = {
    val frame = _versions(position)
    _versions(position) = new Written(frame.txn, frame.out, value)
    frame.out.foreach{ succ => host.taskPool.addChangeNotification(frame.txn, succ) }
    notifyFrameRemovedAndCheckForSubsequentReevaluation(position)
  }

  private def reev_in(position: Int): Option[(Transaction, V)] = {
    val frame = _versions(position)
    _versions(position) = new Active(frame.txn, frame.out)
    val v_in = regReadPred(position)
    Some(frame.txn, v_in)
  }

  private def unchanged(position: Int): Trampoline[Option[(Transaction, V)]] = {
    _versions(position) match {
      case frame: Pending if (frame.pending > 0) =>
        frame.pending -= 1
        if (frame.pending == 0) {
          if (frame.changed > 0) {
            Result(reev_in(position))
          } else {
            Recurse({ () => reev_unchanged(position)})
          }
        } else {
          Result(None)
        }
      case version =>
        throw new IllegalStateException("Cannot process no-change notification - not a Frame: " + version)
    }
  }

  @tailrec
  private def maybeUserCompAndReevOut(maybeReevInResult: Option[(Transaction, V)]): Unit = {
    maybeReevInResult match {
      case Some((txn, v_in)) =>
        val v_out = userComputation(v_in)
        val nextMaybeReevInResult = if (v_in == v_out) synchronized {
          position(txn) match {
            case Found(position) =>
              reev_unchanged(position).bounce
            case _ =>
              throw new AssertionError("Cannot reevOutUnchanged - Frame was deleted during userComputation for " + txn)
          }
        } else synchronized {
          position(txn) match {
            case Found(position) =>
              reev_changed(position, v_out).bounce
            case _ =>
              throw new AssertionError("Cannot reevOutChanged - Frame was deleted during userComputation for " + txn)
          }
        }
        maybeUserCompAndReevOut(nextMaybeReevInResult)
      case None =>
    }
  }

  def unchanged(txn: Transaction): Unit = {
    val maybeReevInResult = synchronized {
      position(txn) match {
        case Found(position) =>
          unchanged(position).bounce
        case _ =>
          throw new IllegalStateException("Cannot process no-change notification - no Frame for " + txn)
      }
    }
    maybeUserCompAndReevOut(maybeReevInResult)
  }

  def changed(txn: Transaction): Unit = synchronized {
    val maybeReevInResult = synchronized {
      position(txn) match {
        case Found(position) =>
          // similar to unchanged(Int), but not abstracted into a method because not relevant for mutual recursion:
          // only the first reevaluation receives a change, within a single node's pipeline, otherwise, only
          // no-change notifications are forwarded, so only unchanged(Int) is required during the tail recursion.
          _versions(position) match {
            case frame: Pending if (frame.pending > 0) =>
              frame.pending -= 1
              if (frame.pending == 0) {
                // frame.changed > 0 is implicit because this itself is a change notification
                reev_in(position)
              } else {
                frame.changed += 1
                None
              }
            case version =>
              throw new IllegalStateException("Cannot process change notification - not a Frame: " + version)
          }
        case _ =>
          throw new IllegalStateException("Cannot process change notification - no Frame for " + txn)
      }
    }
    maybeUserCompAndReevOut(maybeReevInResult)
  }

  // =================== DYNAMIC REWRITE COMPENSATION ====================

  def dechange(txn: Transaction): Unit = synchronized {
    position(txn) match {
      case Found(position) =>
        _versions(position) match {
          case frame: Pending if (frame.pending > 0) =>
            frame.changed -= 1
          case version =>
            throw new IllegalStateException("Cannot process de-change notification - not a Frame: " + version)
        }
      case _ =>
        throw new IllegalStateException("Cannot process de-change notification - no Frame for " + txn)
    }
  }

  // =================== READ OPERATIONS ====================

  private def ensureReadVersion(txn: Transaction): Int = {
    position(txn) match {
      case Found(position) =>
        position
      case InsertionPoint(position) =>
        _versions.insert(position, new ReadVersion(txn, _versions(position - 1).out))
        position
    }
  }

  def before(txn: Transaction): V = synchronized {
    @tailrec
    def before0(txn: Transaction): V = {
      val thisPosition = ensureReadVersion(txn)
      val readPosition = prevReevPosition(thisPosition)
      _versions(readPosition) match {
        case written: Written =>
          written.value
        case frame: Frame =>
          blockUntilNoLongerFrame(frame.txn)
          before0(txn)
        case _ =>
          throw new AssertionError("This case should be impossible; prevReevPosition should only select Written or Frame positions; maybe synchronization bug!")
      }
    }
    before0(txn)
  }

  def now(txn: Transaction): V = synchronized {
    val position = ensureReadVersion(txn)
    _versions(position) match {
      case written: Written => written.value
      case _ =>
        before(txn)
    }
  }


  def after(txn: Transaction): V = synchronized {
    @tailrec
    def after0(txn: Transaction): V = {
      val thisPosition = ensureReadVersion(txn)
      _versions(thisPosition) match {
        case written: Written =>
          written.value
        case frame: Frame =>
          blockUntilNoLongerFrame(frame.txn)
          after0(txn)
        case _ =>
          val prevReev = prevReevPosition(thisPosition)
          _versions(prevReev) match {
            case written: Written =>
              written.value
            case frame: Frame =>
              blockUntilNoLongerFrame(frame.txn)
              after0(txn)
            case version =>
              throw new AssertionError("This case should be impossible, maybe synchronization bug! prevReevPosition should only select Written or Frame positions, but selected " + version)
          }
      }
    }
    after0(txn)
  }

  private def regReadPred(position: Int) = {
    _versions(prevReevPosition(position)) match {
      case written: Written =>
        written.value
      case version: Frame =>
        throw new IllegalStateException("Cannot regRead due to glitch-freedom violation: " + _versions(position).txn + " predecessor has incomplete " + version)
      case version =>
        throw new AssertionError("This case should be impossible, maybe synchronization bug! prevReevPosition should only select Written or Frame positions, but selected " + version)
    }
  }

  def regRead(txn: Transaction): V = synchronized {
    position(txn) match {
      case Found(position) =>
        _versions(position) match {
          case written: Written =>
            written.value
          case version: Frame =>
            throw new IllegalStateException("Cannot regRead due to glitch-freedom violation: transaction has own, but incomplete " + version)
          case version: ReadVersion =>
            regReadPred(position)
        }
      case InsertionPoint(position) =>
        regReadPred(position)
    }
  }

  // =================== DYNAMIC OPERATIONS ====================

  def discover(txn: Transaction, add: O): V = synchronized {
    val position = ensureReadVersion(txn)
    for(pos <- position until _versions.size) {
      val version = _versions(pos)
      version.out += add
      if(version.performedFraming) {
        if(version.performedChangedPropagation) {
          add.changed(version.txn)
        }
      }
    }
    after(txn)
  }

  def drop(txn: Transaction, remove: O): V = synchronized{
    val position = ensureReadVersion(txn)
    for(pos <- position until _versions.size) {
      val version = _versions(pos)
      version.out += remove
      if(version.performedFraming) {
        if(version.performedChangedPropagation) {
          remove.dechange(version.txn)
        }
        maybeDeframe(remove, version.txn)
      }
    }
  }
}
