package reshapes

import scala.swing._
import scala.events.scalareact
import scala.events.behaviour.Var
import util.Random
import scala.swing.event._
import reshapes.figures._
import events.ImperativeEvent
import scala.events.behaviour.Signal
import java.awt.Color

object Reshapes extends SimpleSwingApplication {
  val events = new EventHolder

  val ui = new BorderPanel {

    // GUI Elements and Layout
    val lineBtn = new Button { text = "Line" }
    val rectBtn = new Button { text = "Rectangle" }
    val ovalBtn = new Button { text = "Oval" }
    val strokeWidthInput = new TextField { text = events.strokeWidth.getValue.toString(); columns = 5 }
    val colorInput = new TextField { text = "0,0,0"; columns = 10 }
    val shapePanel = new ShapePanel(events)

    add(new FlowPanel {
      contents += new Label { text = "stroke width: " }
      contents += strokeWidthInput
      contents += new Label { text = "stroke color: " }
      contents += colorInput
    }, BorderPanel.Position.North)

    add(new BoxPanel(Orientation.Vertical) {
      contents += lineBtn
      contents += rectBtn
      contents += ovalBtn
    }, BorderPanel.Position.West)

    add(new DrawingPanel(events), BorderPanel.Position.Center)
    add(new InfoPanel(events), BorderPanel.Position.South)
    add(shapePanel, BorderPanel.Position.East)

    // reactions
    listenTo(lineBtn)
    listenTo(rectBtn)
    listenTo(ovalBtn)
    listenTo(strokeWidthInput)
    listenTo(colorInput)
    listenTo(mouse.clicks)

    reactions += {
      case ButtonClicked(`lineBtn`) =>
        events.nextShape() = new Line
      case ButtonClicked(`rectBtn`) =>
        events.nextShape() = new figures.Rectangle
      case ButtonClicked(`ovalBtn`) =>
        events.nextShape() = new Oval
      case EditDone(`strokeWidthInput`) =>
        try {
          events.strokeWidth() = strokeWidthInput.text.toInt match {
            case i if i > 0 => i
            case _ => strokeWidthInput.text = "1"; 1
          }

          events.mode match {
            case Selection() =>
              events.selectedShape.getValue.strokeWidth = events.strokeWidth.getValue
              repaint()
            case _ =>
          }
        } catch {
          case e: NumberFormatException => strokeWidthInput.text = events.strokeWidth.getValue.toString()
        }
      case EditDone(`colorInput`) =>
        try {
          val input = colorInput.text.split(',') match {
            case empty if empty.length == 1 && empty(0).isEmpty() =>
              events.color() = new Color(0, 0, 0)
              colorInput.text = "0,0,0"
            case rgbStr if rgbStr.length == 3 =>
              val rgb = rgbStr.map(x => x.toInt)
              events.color() = new Color(rgb(0), rgb(1), rgb(2))
            case _ => throw new NumberFormatException
          }

          events.mode match {
            case Selection() =>
              events.selectedShape.getValue.color = events.color.getValue
              repaint()
            case _ =>
          }
        } catch {
          case _ => colorInput.text = "%d,%d,%d".format(events.color.getValue.getRed(), events.color.getValue.getGreen(), events.color.getValue.getBlue())
        }
    }
  }

  val menu = new MenuBar {
    val save = new MenuItem("Save")
    val load = new MenuItem("Load")
    val quit = new MenuItem(Action("Quit") {
      System.exit(0)
    })
    val undo = new MenuItem(Action("Undo") {
      events.Commands.getValue.first.revert()
      events.Commands() = events.Commands.getValue.tail
    }) { enabled = false }

    events.Commands.changed += (commands => undo.enabled = commands.size > 0)

    contents += new Menu("File") {
      contents += save
      contents += load
      contents += new Separator
      contents += quit
    }
    contents += new Menu("Edit") {
      contents += undo
    }
  }

  def top = new MainFrame {
    title = "ReShapes";
    preferredSize = new Dimension(1000, 500)

    menuBar = menu
    contents = ui
  }
}