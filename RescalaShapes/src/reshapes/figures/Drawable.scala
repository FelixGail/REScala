package reshapes.figures

import java.awt.Point
import java.awt.Graphics2D
import java.awt.BasicStroke
import java.awt.Color
import reshapes.util.MathUtil
import java.util.UUID

abstract class Shape extends Serializable {
  Shape.current += 1

  var strokeWidth = 1
  var color = Color.BLACK
  var selected = false
  var current = Shape.current
  // the mouse path while drawing this shape
  var path: List[Point] = null

  def start = if (path == null) null else path.head
  def end = if (path == null) null else path.last

  def draw(g: Graphics2D) = {
    if (start != null && end != null) {
      val stroke = if (!selected) new BasicStroke(strokeWidth) else new BasicStroke(strokeWidth,
        BasicStroke.CAP_BUTT,
        BasicStroke.JOIN_MITER,
        10.0f, Array(10.0f), 0.0f)

      g.setStroke(stroke)
      g.setColor(color)
      doDraw(g)
    }
  }

  def update(path: List[Point]) = {
    this.path = path

    doUpdate(path)
  }

  override def toString(): String = {
    this.getClass().getSimpleName() + " #" + current.toString()
  }

  def doUpdate(path: List[Point]) = {}
  def doDraw(g: Graphics2D)

  /**
   * returns a list of lines representing the shape
   */
  def toLines(): List[(Int, Int, Int, Int)]
}

object Shape {
  private var current = 0
}

trait Movable extends Shape {

  def move(from: Point, to: Point) = {
    val deltaX = (if (from.x < to.x) 1 else -1) * math.abs(from.x - to.x)
    val deltaY = (if (from.y < to.y) 1 else -1) * math.abs(from.y - to.y)

    path = path map (point => new Point(point.x + deltaX, point.y + deltaY))
  }
}

trait Resizable extends Shape {

  def resize(from: Point, to: Point) = {
    if (MathUtil.isInCircle(start, 6, from)) {
      path = to :: path.tail
    } else if (MathUtil.isInCircle(end, 6, from)) {
      path = (to :: path.reverse.tail).reverse
    }
  }

  override def draw(g: Graphics2D) = {
    super.draw(g)

    if (start != null && end != null && selected) {
      val origStroke = g.getStroke()
      g.setStroke(new BasicStroke(1))
      g.setColor(new Color(200, 200, 200))

      g.drawOval(start.x - 5, start.y - 5, 10, 10)
      g.drawOval(end.x - 5, end.y - 5, 10, 10)

      g.setStroke(origStroke)
    }
  }
}
