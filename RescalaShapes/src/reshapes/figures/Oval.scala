package reshapes.figures
import java.awt.Point
import java.awt.Graphics2D

class Oval extends Drawable {

  def update(path: List[Point]) = {
    start = path.first
    end = path.last
  }

  def draw(g: Graphics2D) = {
    var width = math.abs(start.x - end.x)
    var height = math.abs(start.y - end.y)
    var x = math.min(start.x, end.x)
    var y = math.min(start.y, end.y)

    g.drawOval(x, y, width, height)
  }

}