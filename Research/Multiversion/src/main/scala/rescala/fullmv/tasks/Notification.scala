package rescala.fullmv.tasks

import rescala.fullmv.NotificationResultAction._
import rescala.fullmv._
import rescala.graph.Reactive

trait NotificationAction extends ReevaluationResultHandling {
  override def compute(): Unit = {
    val notificationResultAction = deliverNotification()
    if(FullMVEngine.DEBUG) println(s"[${Thread.currentThread().getName}] $this => $notificationResultAction")
    processNotificationResult(notificationResultAction)
  }

  def processNotificationResult(notificationResultAction: NotificationResultAction[FullMVTurn, Reactive[FullMVStruct]]) = {
    notificationResultAction match {
      case GlitchFreeReadyButQueued =>
        turn.activeBranchDifferential(TurnPhase.Executing, -1)
      case ResolvedNonFirstFrameToUnchanged =>
        turn.activeBranchDifferential(TurnPhase.Executing, -1)
      case NotGlitchFreeReady =>
        turn.activeBranchDifferential(TurnPhase.Executing, -1)
      case GlitchFreeReady =>
        Reevaluation(turn, node).fork
      case outAndSucc: NotificationOutAndSuccessorOperation[FullMVTurn, Reactive[FullMVStruct]] =>
        processReevaluationResult(outAndSucc, changed = false)
    }
  }

  def deliverNotification(): NotificationResultAction[FullMVTurn, Reactive[FullMVStruct]]
}

case class Notification(turn: FullMVTurn, node: Reactive[FullMVStruct], changed: Boolean) extends NotificationAction {
  override def deliverNotification(): NotificationResultAction[FullMVTurn, Reactive[FullMVStruct]] = node.state.notify(turn, changed)
}
case class NotificationWithFollowFrame(turn: FullMVTurn, node: Reactive[FullMVStruct], changed: Boolean, followFrame: FullMVTurn) extends NotificationAction {
  override def deliverNotification(): NotificationResultAction[FullMVTurn, Reactive[FullMVStruct]] = node.state.notifyFollowFrame(turn, changed, followFrame)
}
