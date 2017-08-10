package rescala.fullmv.mirrors

import rescala.fullmv.FullMVTurn
import rescala.fullmv.tasks.FullMVAction

sealed trait RemotePushMessage[+T] {
  val txn: FullMVTurn
  def toTask(localReflection: LocalReactiveReflection[T]): FullMVAction
}
object RemotePushMessage {
  case class Framing(txn: FullMVTurn) extends RemotePushMessage[Nothing] {
    override def toTask(localReflection: LocalReactiveReflection[Nothing]): FullMVAction = rescala.fullmv.tasks.Framing(txn, localReflection)
  }
  case class SupersedeFraming(txn: FullMVTurn, supersede: FullMVTurn) extends RemotePushMessage[Nothing] {
    override def toTask(localReflection: LocalReactiveReflection[Nothing]): FullMVAction = rescala.fullmv.tasks.SupersedeFraming(txn, localReflection, supersede)
  }

  sealed trait NotificationMessage[+T] extends RemotePushMessage[T] {
    val changed: Boolean
  }

  sealed trait NoFollowFrameNotificationMessage[+T] extends NotificationMessage[T] {
    override def toTask(localReflection: LocalReactiveReflection[T]): FullMVAction = rescala.fullmv.tasks.Notification(txn, localReflection, changed)
  }
  sealed trait NotificationWithFollowFrameMessage[+T] extends NotificationMessage[T] {
    val followFrame: FullMVTurn
    override def toTask(localReflection: LocalReactiveReflection[T]): FullMVAction = rescala.fullmv.tasks.NotificationWithFollowFrame(txn, localReflection, changed, followFrame)
  }

  sealed trait UnChangedMessage extends NotificationMessage[Nothing] {
    override val changed = false
  }
  sealed trait ChangeMessage[+T] extends NotificationMessage[T] {
    override val changed = true
    val newValue: T
    abstract override def toTask(localReflection: LocalReactiveReflection[T]): FullMVAction = {
      localReflection.buffer(txn, newValue)
      super.toTask(localReflection)
    }
  }

  case class UnchangedNotification(txn: FullMVTurn) extends NoFollowFrameNotificationMessage[Nothing] with UnChangedMessage
  case class UnchangedNotificationWithFollowFrame(txn: FullMVTurn, followFrame: FullMVTurn) extends NotificationWithFollowFrameMessage[Nothing] with UnChangedMessage

  case class ChangeNotification[T](txn: FullMVTurn, newValue: T) extends NoFollowFrameNotificationMessage[T] with ChangeMessage[T]
  case class ChangeNotificationWithFollowFrame[T](txn: FullMVTurn, newValue: T, followFrame: FullMVTurn) extends NotificationWithFollowFrameMessage[T] with ChangeMessage[T]
}
