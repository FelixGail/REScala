package reshapes.ui.panels
import java.awt.BasicStroke

import scala.annotation.serializable
import scala.collection.mutable.MutableList
import scala.events.behaviour.Var
import scala.events.behaviour.Signal
import scala.swing.event.MouseDragged
import scala.swing.event.MousePressed
import scala.swing.event.MouseReleased
import scala.swing.Color
import scala.swing.Graphics2D
import scala.swing.Point
import scala.swing.Panel
import scala.util.Marshal

import reshapes.command.CreateShape
import reshapes.command.EditShape
import reshapes.figures.Movable
import reshapes.figures.Resizable
import reshapes.figures.Shape
import reshapes.util.MathUtil
import reshapes.Drawing
import reshapes.DrawingSpaceState
import reshapes.Reshapes
import reshapes.Selection

/**
 * Represents the panel where all shapes are drawn onto.
 */
class DrawingPanel(val event: DrawingSpaceState) extends Panel {
  opaque = true

  var currentPath = new Var[List[Point]](List())
  var shapes = List[Shape]()
  val currentShape = Signal { Reshapes.CurrentEvents().nextShape() }
  var currentlyDrawing: Shape = null
  var shapeBeforeEdit: Shape = null
  var resizingMode = false

  override def paint(g: Graphics2D) = {
    g.setColor(java.awt.Color.WHITE)
    g.fillRect(0, 0, size.getWidth().toInt, size.getHeight().toInt)

    g.setColor(java.awt.Color.BLACK)
    Reshapes.CurrentEvents.getValue.allShapes.getValue.map(x => x.draw(g))
    if (currentlyDrawing != null) {
      currentlyDrawing.draw(g)
    }
  }

  listenTo(mouse.clicks)
  listenTo(mouse.moves)

  reactions += {
    case e: MousePressed =>
      currentPath() = List(e.point)
      Reshapes.CurrentEvents.getValue.mode match {
        case Drawing() =>
          currentlyDrawing = currentShape.getValue.getClass().newInstance()
          currentlyDrawing.strokeWidth = Reshapes.CurrentEvents.getValue.strokeWidth.getValue
          currentlyDrawing.color = Reshapes.CurrentEvents.getValue.color.getValue
        case Selection() =>
          shapeBeforeEdit = Marshal.load[Shape](Marshal.dump[Shape](Reshapes.CurrentEvents.getValue.selectedShape.getValue)) // hack to get a object copy
          Reshapes.CurrentEvents.getValue.mode match {
            case Drawing() =>
              currentlyDrawing.update(currentPath.getValue)
            case Selection() =>
              val shape = Reshapes.CurrentEvents.getValue.selectedShape.getValue
              resizingMode = MathUtil.isInCircle(shape.start, 6, e.point) || MathUtil.isInCircle(shape.end, 6, e.point)
            case _ =>
          }
        case _ =>
      }
      repaint()
    case e: MouseDragged =>
      currentPath() = currentPath.getValue ::: List(e.point)
      Reshapes.CurrentEvents.getValue.mode match {
        case Drawing() =>
          currentlyDrawing.update(currentPath.getValue)
        case Selection() =>
          val shape = Reshapes.CurrentEvents.getValue.selectedShape.getValue
          if (resizingMode && shape.isInstanceOf[Resizable]) {
            shape.asInstanceOf[Resizable].resize(currentPath.getValue.reverse(1), e.point)
          } else if (shape.isInstanceOf[Movable]) {
            shape.asInstanceOf[Movable].move(currentPath.getValue.reverse(1), e.point)
          }
        case _ =>
      }
      repaint()
    case e: MouseReleased =>
      Reshapes.CurrentEvents.getValue.mode match {
        case Drawing() =>
          new CreateShape(currentlyDrawing).execute()
          currentlyDrawing = null
        case Selection() =>
          var command = new EditShape(shapeBeforeEdit, Reshapes.CurrentEvents.getValue.selectedShape.getValue)
          command.execute()
        case _ =>
      }
  }
}

/**
 * This trait draws intersection points between all drawn shapes.
 */
trait ShowIntersection extends DrawingPanel {
  override def paint(g: Graphics2D) = {
    super.paint(g)

    g.setColor(new Color(255, 0, 0))
    g.setStroke(new BasicStroke())
    getIntersectionPoints map (point => g.drawOval(point._1 - 3, point._2 - 3, 6, 6))
  }

  def getIntersectionPoints(): List[(Int, Int)] = {
    val points = MutableList[(Int, Int)]()

    if (Reshapes.CurrentEvents.getValue.allShapes.getValue.size == 0)
      return points.toList

    for (shape <- Reshapes.CurrentEvents.getValue.allShapes.getValue) {
      for (otherShape <- Reshapes.CurrentEvents.getValue.allShapes.getValue.filter(s => s != shape)) {
        for (line <- shape.toLines()) {
          for (otherline <- otherShape.toLines()) {
            val intersection = MathUtil.getIntersectionsOfTwoLines(line, otherline)
            if (intersection != null)
              points += intersection
          }
        }
      }
    }

    points.toList
  }
}

/**
 * Draws a coordinate System onto the panel.
 */
trait ShowCoordinateSystem extends DrawingPanel {
  override def paint(g: Graphics2D) = {
    super.paint(g)

    g.setColor(new Color(200, 200, 200))
    g.setStroke(new BasicStroke())
    for (i <- 0 until size.height if i % 20 == 0) {
      g.drawLine(0, i, size.width, i)
      g.drawString((i / 20).toString(), 0, i)
    }
    for (i <- 0 until size.width if i % 20 == 0) {
      g.drawLine(i, 0, i, size.height)
      g.drawString((i / 20).toString(), i, 10)
    }
  }
}

/**
 * Writes the name of the shape besides them on drawing panel.
 */
trait ShowNameLabels extends DrawingPanel {
  override def paint(g: Graphics2D) = {
    super.paint(g)

    g.setColor(new Color(200, 200, 200))
    g.setStroke(new BasicStroke())
    Reshapes.CurrentEvents.getValue.allShapes.getValue map (shape => g.drawString(shape.toString(), shape.start.x, shape.start.y))
  }
}

