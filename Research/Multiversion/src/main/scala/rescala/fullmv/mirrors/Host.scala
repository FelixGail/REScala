package rescala.fullmv.mirrors

import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap, ThreadLocalRandom}

import rescala.fullmv.FullMVTurn
import rescala.fullmv.mirrors.Host.GUID
import rescala.fullmv.sgt.synchronization.{SubsumableLock, SubsumableLockImpl}

import scala.annotation.tailrec

trait Hosted {
  val host: Host[_]
  val guid: Host.GUID
  override def equals(obj: scala.Any): Boolean = obj.isInstanceOf[Hosted] && obj.asInstanceOf[Hosted].guid == guid
  val hc: Int = (guid ^ (guid >>> 32)).toInt
  override def hashCode(): Int = hc
}

object Host {
  type GUID = Long
  val dummyGuid: GUID = 0L
  val DEBUG = false
}
sealed trait CacheResult[T] { val instance: T }
case class Found[T](instance: T) extends CacheResult[T]
case class Instantiated[T](instance: T) extends CacheResult[T]
trait Host[T] {
  val dummy: T
  def getInstance(guid: Host.GUID): Option[T]
  def getCachedOrReceiveRemote(guid: Host.GUID, instantiateReflection: => T): CacheResult[T]
  def dropInstance(guid: GUID, instance: T): Unit
  def createLocal[U <: T](create: Host.GUID => U): U
}

trait HostImpl[T] extends Host[T] {
  val instances: ConcurrentMap[GUID, T] = new ConcurrentHashMap()
  override def getInstance(guid: GUID): Option[T] = Option(instances.get(guid))
  override def getCachedOrReceiveRemote(guid: Host.GUID, instantiateReflection: => T): CacheResult[T] = {
    @inline @tailrec def findOrReserveInstance(): T = {
      val found = instances.putIfAbsent(guid, dummy)
      if(found != dummy) {
        found
      } else {
        if(Host.DEBUG) println(s"[${Thread.currentThread().getName}] on $this cache contended for $guid")
        Thread.`yield`()
        findOrReserveInstance()
      }
    }
    val known = findOrReserveInstance()
    if(known != null) {
      if(Host.DEBUG) println(s"[${Thread.currentThread().getName}] on $this cache hit $known")
      Found(known)
    } else {
      if(Host.DEBUG) println(s"[${Thread.currentThread().getName}] on $this cache miss for $guid, invoking instantiation...")
      val instance = instantiateReflection
      val replaced = instances.replace(guid, dummy, instance)
      assert(replaced, s"someone stole the dummy placeholder while instantiating remotely received $guid on $this!")
      if(Host.DEBUG) println(s"[${Thread.currentThread().getName}] on $this cached $instance")
      Instantiated(instance)
    }
  }
  override def dropInstance(guid: GUID, instance: T): Unit = {
    val removed = instances.remove(guid, instance)
    assert(removed, s"removal of $instance on $this failed")
    if(Host.DEBUG) println(s"[${Thread.currentThread().getName}] deallocated $instance")
  }
  override def createLocal[U <: T](create: GUID => U): U = {
    @inline @tailrec def redoId(): GUID = {
      val id = ThreadLocalRandom.current().nextLong()
      if(id == Host.dummyGuid) {
        redoId()
      } else {
        val known = instances.putIfAbsent(id, dummy)
        if(known == null) id else redoId()
      }
    }
    val guid = redoId()
    val instance = create(guid)
    val replaced = instances.replace(guid, dummy, instance)
    assert(replaced, s"someone stole the dummy placeholder while creating new instance $guid on $this!")
    if(Host.DEBUG) println(s"[${Thread.currentThread().getName}] on $this created local instance $instance")
    instance
  }
}
trait SubsumableLockHost extends Host[SubsumableLock] {
  def getCachedOrReceiveRemoteWithReference(guid: Host.GUID, remoteProxy: => SubsumableLockProxy): SubsumableLock = {
    @tailrec def retry(): SubsumableLock = {
      getCachedOrReceiveRemote(guid, new SubsumableLockReflection(this, guid, remoteProxy)) match {
        case Instantiated(instance) =>
          if (SubsumableLock.DEBUG) println(s"[${Thread.currentThread().getName}] $this cache miss for remote lock reception, newly instantiated $instance")
          instance
        case Found(instance) =>
          if (instance.tryLocalAddRefs(1)) {
            if (SubsumableLock.DEBUG) println(s"[${Thread.currentThread().getName}] $this cache hit for remotely received $instance, dropping connection establishment remote reference on $remoteProxy")
            remoteProxy.asyncRemoteRefDropped()
            instance
          } else {
            if (SubsumableLock.DEBUG) println(s"[${Thread.currentThread().getName}] $this cache hit for remotely received $instance, but was concurrently deallocated; retrying cache lookup.")
            retry()
          }
      }
    }
    retry()

  }
}

class SubsumableLockHostImpl extends SubsumableLockHost with HostImpl[SubsumableLock] {
  override val dummy = new SubsumableLockImpl(this, Host.dummyGuid)
  instances.put(0, dummy)
  dummy.localSubRefs(1)
  if(Host.DEBUG || SubsumableLock.DEBUG) println(s"[${Thread.currentThread().getName}] $this SETUP COMPLETE")
  def newLock(): SubsumableLockImpl = createLocal(new SubsumableLockImpl(this, _))
}
trait FullMVTurnHost extends Host[FullMVTurn] {
  val lockHost: SubsumableLockHost
}
