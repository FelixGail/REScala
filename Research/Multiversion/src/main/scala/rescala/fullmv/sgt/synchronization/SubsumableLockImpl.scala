package rescala.fullmv.sgt.synchronization

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.LockSupport

import rescala.fullmv.mirrors.{Host, SubsumableLockHost}
import rescala.fullmv.sgt.synchronization.SubsumableLock._
import rescala.parrp.Backoff

import scala.annotation.tailrec

class SubsumableLockImpl(override val host: SubsumableLockHost, override val guid: Host.GUID) extends SubsumableLock {
  Self =>
  val state = new AtomicReference[SubsumableLock](null)

  override def getLockedRoot: Option[Host.GUID] = {
    state.get match {
      case null => None
      case Self => Some(guid)
      case parent => parent.getLockedRoot
    }
  }

  override def tryLock(): TryLockResult = {
    state.get match {
      case null =>
        val success = state.compareAndSet(null, Self)
        if(DEBUG) println(s"[${Thread.currentThread().getName}]: ${if(success) "trylocked" else "failed trylock to contention"} $this")
        TryLockResult(success, this, guid)
      case Self =>
        if(DEBUG) println(s"[${Thread.currentThread().getName}]: trylock $this blocked")
        TryLockResult(success = false, this, guid)
      case parent =>
        val res = parent.tryLock()
        state.compareAndSet(parent, res.newParent)
        res
    }
  }

  override def trySubsume(lockedNewParent: TryLockResult): Option[SubsumableLock] = {
    assert(lockedNewParent.newParent.host == host, s"trySubsume $this to ${lockedNewParent.newParent} is hosted on ${lockedNewParent.newParent.host} different from $host")
    if(lockedNewParent.globalRoot == this.guid) {
      assert(state.get == Self, s"passed in a TryLockResult indicates that $this was successfully locked, but it currently isn't!")
      if (DEBUG) println(s"[${Thread.currentThread().getName}]: trySubsume $this to itself reentrant success")
      None
    } else {
      state.get match {
        case null =>
          val success = state.compareAndSet(null, lockedNewParent.newParent)
          if(success) {
            if (DEBUG) println(s"[${Thread.currentThread().getName}]: trySubsume $this succeeded")
            None
          } else {
            if (DEBUG) println(s"[${Thread.currentThread().getName}]: trySubsume $this to ${lockedNewParent.newParent} failed to contention")
            Some(this)
          }
        case Self =>
          if (DEBUG) println(s"[${Thread.currentThread().getName}]: trySubsume $this to ${lockedNewParent.newParent} blocked")
          Some(this)
        case parent =>
          val res = parent.trySubsume(lockedNewParent)
          state.compareAndSet(parent, res.getOrElse(lockedNewParent.newParent))
          res
      }
    }
  }

  val waiters = new ConcurrentLinkedQueue[Thread]()
  @tailrec final override def lock(): TryLockResult = {
    state.get match {
      case null =>
        if(state.compareAndSet(null, Self)) {
          if(DEBUG) println(s"[${Thread.currentThread().getName}]: locked $this")
          TryLockResult(success = true, this, guid)
        } else {
          if(DEBUG) println(s"[${Thread.currentThread().getName}]: retrying contended lock attempt of $this")
          lock()
        }
      case Self =>
        val thread = Thread.currentThread()
        if(DEBUG) println(s"[${Thread.currentThread().getName}]: waiting for lock on $this")
        waiters.add(thread)

        while(waiters.peek() != thread || state.get == Self) {
          if(DEBUG) println(s"[${Thread.currentThread().getName}]: parking on $this")
          LockSupport.park(this)
          if(DEBUG) println(s"[${Thread.currentThread().getName}]: unparked on $this")
        }

        waiters.remove()
        val s = state.get
        if(s != null && s != Self) {
          val peeked = waiters.peek()
          if(DEBUG) println(s"[${Thread.currentThread().getName}]: previous owner subsumed $this, moving on " + (if(peeked == null) "without successor" else "after also unparking successor "+peeked.getName))
          LockSupport.unpark(peeked)
        }
        lock()
      case parent =>
        val res = parent.lock()
        state.compareAndSet(parent, res.newParent)
        res
    }
  }

  override def unlock(): Unit = synchronized {
    assert(state.get != null, s"unlock attempt on non-locked $this")
    assert(state.get == Self, s"unlock attempt on subsumed $this")
    if(!state.compareAndSet(Self, null)) throw new AssertionError(s"$this unlock failed due to contention!?")
    val peeked = waiters.peek()
    if(DEBUG) println(s"[${Thread.currentThread().getName}]: release $this, unparking $peeked")
    LockSupport.unpark(peeked)
  }


  override def spinOnce(backoff: Long): TryLockResult = {
    // this method may seem silly, but serves as an local redirect for preventing back-and-forth remote messages.
    state.get match {
      case null =>
        throw new IllegalStateException(s"spinOnce on unlocked $this")
      case Self =>
        unlock()
        Backoff.backoff(backoff)
        lock()
      case parent =>
        val res = parent.spinOnce(backoff)
        state.compareAndSet(parent, res.newParent)
        res
    }
  }

  override def subsume(lockedNewParent: TryLockResult): Unit = synchronized {
    assert(lockedNewParent.newParent.host == host, s"subsume $this to ${lockedNewParent.newParent} is hosted on ${lockedNewParent.newParent.host} different from $host")
    assert(lockedNewParent.success, s"trying to subsume on failed lock")
    assert(lockedNewParent.globalRoot != guid, s"trying to create endless loops, are you?")
    assert(lockedNewParent.newParent.getLockedRoot.isDefined, s"subsume partner $lockedNewParent is unlocked.")
    assert(state.get != null, s"subsume attempt on unlocked $this")
    assert(state.get == Self, s"subsume attempt on subsumed $this")
    if(!state.compareAndSet(Self, lockedNewParent.newParent)) throw new AssertionError(s"$this subsume failed due to contention!?")
    val peeked = waiters.peek()
    if(DEBUG) println(s"[${Thread.currentThread().getName}]: subsumed $this, unparking " + (if(peeked == null) "none" else peeked.getName))
    LockSupport.unpark(peeked)
  }

  override def toString = s"Lock($guid on $host, ${state.get match {
      case null => "unlocked"
      case Self => "locked"
      case other => s"subsumed($other)"
    }})"
}
