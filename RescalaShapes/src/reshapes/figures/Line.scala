package reshapes.figures
import java.awt.Point
import java.awt.Graphics2D
import java.awt.Color
import reshapes.drawing.DrawingSpaceState

class Line(
  drawingSpaceState: DrawingSpaceState,
  strokeWidth: Int = 1,
  color: Color = Color.BLACK,
  current: Int = 0,
  path: List[Point] = null)
    extends Shape(drawingSpaceState, strokeWidth, color, current, path) with Movable with Resizable {
  def doDraw(g: Graphics2D) = {
    g.drawLine(start.x, start.y, end.x, end.y)
  }
  
  def toLines(): List[(Int, Int, Int, Int)] = {
    List((start.x, start.y, end.x, end.y))
  }
  
  override def copy(
      drawingSpaceState: DrawingSpaceState,
      strokeWidth: Int, 
      color: Color,
      current: Int,
      path: List[Point]) =
    new Line(drawingSpaceState, strokeWidth, color, current, path)
}