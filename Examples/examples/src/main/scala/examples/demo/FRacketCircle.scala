package examples.demo

import java.awt.Color

import examples.demo.ui._
import rescala._

object FRacketCircle extends Main {
  class BouncingCircle(val diameter: Signal[Int], val reset: Event[Point]) {
    val physicsTicks = Clock.time.change.map{ diff => diff.to.get - diff.from.get }

    val angle = Signal{ Clock.time().toDouble / Clock.NanoSecond }

    val horizontalBounceSources: Var[List[Event[Any]]] = Var(List())
    val verticalBounceSources: Var[List[Event[Any]]] = Var(List())

    val velocityX = horizontalBounceSources.flatten.fold(150d / Clock.NanoSecond) { (old, _) => -old }
    val velocityY = verticalBounceSources.flatten.fold(100d / Clock.NanoSecond) { (old, _ ) => -old }

    val posX = reset.zipOuter(physicsTicks).fold(0d){
      case (_, (Some(Point(x, _)), _)) => x.toDouble
      case (pX, (None, Some(tick))) => pX + tick * velocityX.before
    }
    val posY = reset.zipOuter(physicsTicks).fold(0d){
      case (_, (Some(Point(_, y)), _)) => y.toDouble
      case (pY, (None, Some(tick))) => pY + tick * velocityY.before
    }

    val shape = new Circle(posX.map(_.toInt), posY.map(_.toInt), diameter)
  }

  class BoundingBox(val width: Signal[Int], val height: Signal[Int], val boundShape: Shape) {
    val centerBBHorizontalDistance = Signal{ (width() - boundShape.hitboxWidth()) / 2 }
    val centerBBVerticalDistance = Signal{ (height() - boundShape.hitboxHeight()) / 2 }

    val outOfBoundsLeft = Signal{ boundShape.centerX() < -centerBBHorizontalDistance() }
    val outOfBoundsRight = Signal{ boundShape.centerX() > centerBBHorizontalDistance() }
    val outOfBoundsTop = Signal{ boundShape.centerY() < -centerBBVerticalDistance() }
    val outOfBoundsBottom = Signal{ boundShape.centerY() > centerBBVerticalDistance() }

    val movedOutOfBoundsLeft = outOfBoundsLeft.changedTo(true)
    val movedOutOfBoundsRight = outOfBoundsRight.changedTo(true)
    val movedOutOfBoundsHorizontal = movedOutOfBoundsLeft || movedOutOfBoundsRight
    val movedOutOfBoundsVertical = Signal{ outOfBoundsTop() || outOfBoundsBottom() }.changedTo(true)

    val shape = new Rectangle(Var(0), Var(0), width, height, Signal{
      if(outOfBoundsLeft() || outOfBoundsRight() || outOfBoundsTop() || outOfBoundsBottom())
        Some(Color.RED) else Some(Color.GREEN)
    })
  }

  class Racket(val fieldHeight: Signal[Int], val fieldWidth: Signal[Int], val height: Signal[Int], val isRight: Boolean, val inputY: Signal[Int]) {
    val width = Var(10)

    val posX = fieldWidth.map(w => (if(isRight) 1 else -1) * (w / 2 - 25))
    val posY = Signal{ math.max(math.min(inputY(), (fieldHeight() - height()) / 2), - (fieldHeight() - height()) / 2) }

    val shape = new Rectangle(posX, posY, width, height)
  }

  val fieldWidth = panel.width.map(_ - 25)
  val fieldHeight = panel.height.map(_ - 25)

  override def makeShapes() = {
    val bouncingCircle = new BouncingCircle(Var(50), panel.Mouse.middleButton.pressed)
    val boundingBox = new BoundingBox(fieldWidth, fieldHeight, bouncingCircle.shape)
    val racket = new Racket(fieldHeight, fieldWidth, Var(100), true, panel.Mouse.y)

    bouncingCircle.horizontalBounceSources.transform(boundingBox.movedOutOfBoundsHorizontal :: _)
    bouncingCircle.verticalBounceSources.transform(boundingBox.movedOutOfBoundsVertical :: _)

    List(bouncingCircle.shape, boundingBox.shape, racket.shape)
  }
}
