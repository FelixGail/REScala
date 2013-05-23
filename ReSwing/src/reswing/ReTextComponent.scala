package reswing

import java.awt.Dimension

import scala.events.ImperativeEvent
import scala.swing.TextComponent
import scala.swing.event.CaretUpdate
import scala.swing.event.ValueChanged

class ReTextComponent extends ReComponent {
  override protected lazy val peer = new TextComponent with ComponentMixin
  
  val text: ImperativeSignal[String] = ImperativeSignal.noSignal
  connectSignal(text, peer.text, peer.text_=)
  
  final val selected: ImperativeSignal[String] = ImperativeSignal.noSignal
  
  final val valueChanged = new ImperativeEvent[ValueChanged]
  
  peer.reactions += {
    case e @ ValueChanged(_) =>
      text(peer.text)
      valueChanged(e)
  }
  
  peer.caret.reactions += {
    case e @ CaretUpdate(_) =>
      selected(peer.selected)
  }
  
  class ReCaret {
    protected lazy val peer = ReTextComponent.this.peer.caret
    
    final val dot: ImperativeSignal[Int] = ImperativeSignal.noSignal(peer.dot)
    final val mark: ImperativeSignal[Int] = ImperativeSignal.noSignal(peer.mark)
    final val position: ImperativeSignal[Int] = ImperativeSignal.noSignal(peer.position)
  
    final val caretUpdate = new ImperativeEvent[CaretUpdate]
    
    peer.reactions += {
      case e @ CaretUpdate(_) =>
        dot(peer.dot)
        mark(peer.mark)
        position(peer.position)
        caretUpdate(e)
    }
  }
  
  object ReCaret {
    implicit def toCaret(input : ReCaret) = input.peer
  }
  
  object caret extends ReCaret
}

object ReTextComponent {
  implicit def toTextComponent(input : ReTextComponent) : TextComponent = input.peer
  
  def apply(
      minimumSize: ImperativeSignal[Dimension] = ImperativeSignal.noSignal,
      maximumSize: ImperativeSignal[Dimension] = ImperativeSignal.noSignal,
      preferredSize: ImperativeSignal[Dimension] = ImperativeSignal.noSignal) =
        Macros.defaultObjectCreation
}
