package reshapes.ui.panels

import scala.events.behaviour.Signal
import scala.swing.BoxPanel
import scala.swing.Orientation

import reshapes.ReShapes
import reshapes.figures.Freedraw
import reshapes.figures.Line
import reshapes.figures.Oval
import reshapes.figures.Rectangle
import reshapes.figures.Shape
import reshapes.figures.Triangle
import reswing.ReButton

/**
 * Panel for selection of shapes to draw
 */
class ShapeSelectionPanel extends BoxPanel(Orientation.Vertical) {
  def state = ReShapes.drawingSpaceState.getValue
  
  val lineBtn = ReButton("Line")
  val rectBtn = ReButton("Rectangle")
  val ovalBtn = ReButton("Oval")
  val triangleBtn = ReButton("Triangle")
  val freedrawBtn = ReButton("Freedraw")
  
  contents += lineBtn
  contents += rectBtn
  contents += ovalBtn
  contents += triangleBtn
  contents += freedrawBtn
  
  val nextShape: Signal[Shape] =
	  ((lineBtn.clicked map {_: Any => new Line(state) }) ||
	   (rectBtn.clicked map {_: Any => new Rectangle(state) }) ||
	   (ovalBtn.clicked map {_: Any => new Oval(state) }) ||
	   (triangleBtn.clicked map {_: Any => new Triangle(state) }) ||
	   (freedrawBtn.clicked map {_: Any => new Freedraw(state) })) latest { new Line(state) }
}