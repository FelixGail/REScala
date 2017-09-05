package examples.demo.ui

import java.awt.{Color, Graphics2D}

import rescala._

trait Shape extends Serializable {
  val changed: Event[Any]
  def centerX: Signal[Int]
  def centerY: Signal[Int]
  def hitboxWidth: Signal[Int]
  def hitboxHeight: Signal[Int]
  def drawSnapshot(g: Graphics2D)(implicit turn: AdmissionTicket): Unit
}

class Circle (override val centerX: Signal[Int],
              override val centerY: Signal[Int],
              val diameter: Signal[Int],
              val border: Signal[Option[Color]] = Var(Some(Color.BLACK)),
              val fill: Signal[Option[Color]] = Var(None)) extends Shape {
  override val changed = centerX.changed || centerY.changed || diameter.changed || border.changed || fill.changed
  override val hitboxWidth = diameter
  override val hitboxHeight = diameter
  override def drawSnapshot(g: Graphics2D)(implicit turn: AdmissionTicket): Unit = {
    val d = turn.now(diameter)
    val x = turn.now(centerX) - d/2
    val y = turn.now(centerY) - d/2
    val f = turn.now(fill)
    if(f.isDefined) {
      g.setColor(f.get)
      g.fillOval(x, y, d, d)
    }
    val b = turn.now(border)
    if(b.isDefined) {
      g.setColor(b.get)
      g.drawOval(x, y, d, d)
    }
  }
}

class Rectangle (override val centerX: Signal[Int],
                 override val centerY: Signal[Int],
                 override val hitboxWidth: Signal[Int],
                 override val hitboxHeight: Signal[Int],
                 val border: Signal[Option[Color]] = Var(Some(Color.BLACK)),
                 val fill: Signal[Option[Color]] = Var(None)) extends Shape {
  override val changed = centerX.changed || centerY.changed || hitboxWidth.changed || hitboxHeight.changed || border.changed || fill.changed
  override def drawSnapshot(g: Graphics2D)(implicit turn: AdmissionTicket): Unit = {
    val w = turn.now(hitboxWidth)
    val h = turn.now(hitboxHeight)
    val x = turn.now(centerX) - w/2
    val y = turn.now(centerY) - h/2
    val f = turn.now(fill)
    if(f.isDefined) {
      g.setColor(f.get)
      g.fillRect(x, y, w, h)
    }
    val b = turn.now(border)
    if(b.isDefined) {
      g.setColor(b.get)
      g.drawRect(x, y, w, h)
    }
  }
}
