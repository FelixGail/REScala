package reswing

import java.awt.Dimension

import scala.swing.Action
import scala.swing.Button

class ReButton(action: Action = null) extends ReAbstractButton {
  override protected lazy val peer = new Button(text.getValue) with AbstractButtonMixin
  
  if (action != null)
    peer.action = action
}

object ReButton {
  implicit def toButton(input : ReButton) : Button = input.peer
  
  def apply(
      text: ImperativeSignal[String] = ImperativeSignal.noSignal,
      action: Action = null,
      minimumSize: ImperativeSignal[Dimension] = ImperativeSignal.noSignal,
      maximumSize: ImperativeSignal[Dimension] = ImperativeSignal.noSignal,
      preferredSize: ImperativeSignal[Dimension] = ImperativeSignal.noSignal) = Macros.applyBody
}
