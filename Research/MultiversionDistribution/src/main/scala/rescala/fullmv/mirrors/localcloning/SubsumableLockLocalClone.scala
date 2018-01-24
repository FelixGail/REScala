package rescala.fullmv.mirrors.localcloning

import rescala.fullmv.FullMVEngine
import rescala.fullmv.mirrors._
import rescala.fullmv.sgt.synchronization.SubsumableLock

import scala.concurrent.Future

object SubsumableLockLocalClone {
  def apply(subsumableLock: SubsumableLock, reflectionHost: SubsumableLockHost): SubsumableLock = {
    val mirrorHost = subsumableLock.host
    val localProxy: SubsumableLockProxy = subsumableLock
    val remoteProxy = new SubsumableLockLocalCloneProxy(mirrorHost, localProxy, reflectionHost)
    reflectionHost.getCachedOrReceiveRemoteWithReference(subsumableLock.guid, remoteProxy)
  }
}

class SubsumableLockLocalCloneProxy(mirrorHost: SubsumableLockHost, localProxy: SubsumableLockProxy, reflectionHost: SubsumableLockHost) extends SubsumableLockProxy {
  override def getLockedRoot: Future[Option[Host.GUID]] = localProxy.getLockedRoot
  override def remoteTryLock(): Future[RemoteTryLockResult] = localProxy.remoteTryLock().map {
    case RemoteLocked(newParent: SubsumableLock) => RemoteLocked(SubsumableLockLocalClone(newParent, reflectionHost))
    case RemoteBlocked(newParent: SubsumableLock) => RemoteBlocked(SubsumableLockLocalClone(newParent, reflectionHost))
    case RemoteGCd => RemoteGCd
  }(FullMVEngine.notWorthToMoveToTaskpool)
  override def remoteTrySubsume(lockedNewParent: SubsumableLock): Future[RemoteTrySubsumeResult] = {
    val parameterOnMirrorHostWithTemporaryReference = SubsumableLockLocalClone(lockedNewParent, mirrorHost)
    localProxy.remoteTrySubsume(parameterOnMirrorHostWithTemporaryReference).map { res =>
      val transferRes = res match {
        case RemoteSubsumed => RemoteSubsumed
        case RemoteBlocked(newParent: SubsumableLock) => RemoteBlocked(SubsumableLockLocalClone(newParent, reflectionHost))
        case RemoteGCd => RemoteGCd
      }
      parameterOnMirrorHostWithTemporaryReference.localSubRefs(1)
      transferRes
    }(FullMVEngine.notWorthToMoveToTaskpool)
  }
  override def remoteAsyncUnlock(): Unit = localProxy.remoteAsyncUnlock()
  override def asyncRemoteRefDropped(): Unit = localProxy.asyncRemoteRefDropped()
}
