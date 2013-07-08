package millgame.versions.events

import millgame.types._
import millgame.versions.events._
import react.events.ImperativeEvent

abstract class Gamestate {
  def getPlayer: Slot
  def text: String
}

case class PlaceStone(val player: Slot) extends Gamestate {
  def getPlayer = player
  def text = player + " to PLACE a stone"
}

case class RemoveStone(val player: Slot) extends Gamestate {
  def getPlayer = player
  def color = player.other
  def text = player + " to REMOVE a " + color + " stone"
}

case class MoveStoneSelect(val player: Slot) extends Gamestate {
  def getPlayer = player
  def text = player + " to MOVE a stone"
}

case class MoveStoneDrop(val player: Slot, index: Int) extends Gamestate {
  def getPlayer = player
  def text = player + " to select destination for stone " + index
}

case class JumpStoneSelect(val player: Slot) extends Gamestate {
  def getPlayer = player
  def text = player + " to JUMP with a stone"
}
case class JumpStoneDrop(val player: Slot, index: Int) extends Gamestate {
  def getPlayer = player
  def text = player + " to select destination for stone " + index
}

case class GameOver(winner: Slot) extends Gamestate {
  def getPlayer = Empty
  def text = "Game over. " + winner + " wins."
}

class MillGame {

  val board = new MillBoard
  var state: Gamestate = PlaceStone(White)
  var remainCount: Map[Slot, Int] = Map(Black -> 4, White -> 4)
  
  def stateText = state.text 

  val remainCountChanged = new ImperativeEvent[Map[Slot, Int]]

  val gameWon = new ImperativeEvent[Slot]

  val stateChanged = new ImperativeEvent[Gamestate]
  private def changeState(to: Gamestate) {
    state = to
    stateChanged(state)
  }
  
   
  /* Event based game logic: */
   board.millClosed += { color =>
    changeState(RemoveStone(color))
  }

  board.numStonesChanged += {
    case (color, n) =>
      if (remainCount(color) == 0 && n < 3) {
        gameWon(color.other)
        changeState(GameOver(color.other))
      }
  }
  

  private def nextState(player: Slot): Gamestate =
    if (remainCount(player) > 0) PlaceStone(player)
    else if (board.numStones(player) == 3) JumpStoneSelect(player)
    else MoveStoneSelect(player)

  private def decrementCount(player: Slot) {
    remainCount = remainCount.updated(player, remainCount(player) - 1)
    remainCountChanged(remainCount)
  }

  def playerInput(i: Int): Boolean = state match {

    case PlaceStone(player) =>
      if (board.canPlace(i)) {
        changeState(nextState(player.other))
        decrementCount(player)
        board.place(i, player)
        true
      } else false

    case remove @ RemoveStone(player) =>
      if (board(i) == remove.color) {
        changeState(nextState(player.other))
        /// NOTE: Removing the stone can trigger events which change the state
        /// therefore, remove has to be called after the change state
        board.remove(i) 
        true
      } else false

    case MoveStoneSelect(player) =>
      if (board(i) == player) {
        changeState(MoveStoneDrop(player, i))
        true
      } else false

    case MoveStoneDrop(player, stone) =>
      if (board.canMove(stone, i)) {
        changeState(nextState(player.other))
        board.move(stone, i)
        true
      } else {
        changeState(MoveStoneSelect(player))
        false
      }

    case JumpStoneSelect(player) =>
      if (board(i) == player) {
        changeState(JumpStoneDrop(player, i))
        true
      } else false

    case JumpStoneDrop(player, stone) =>
      if (board.canJump(stone, i)) {
        changeState(nextState(player.other))
        board.move(stone, i)
        true
      } else {
        changeState(MoveStoneSelect(player))
        false
      }

    case _ => false
  }
}