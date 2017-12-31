package reshapes.actions

import java.io.File

import scala.swing.Action
import scala.swing.FileChooser
import scala.xml.XML

import reshapes.ReShapes
import reshapes.drawing.CreateShape
import reshapes.figures.Shape
import scala.language.reflectiveCalls
import rescala._

/**
 * Serializes all currently drawn shapes to a chosen file
 */
class SaveAction extends Action("Save") {
  def apply() = {
    val fileChooser = new FileChooser()
    fileChooser.selectedFile = new File(ReShapes.drawingSpaceState.now.fileName.now)
    if (fileChooser.showDialog(null, "save") == FileChooser.Result.Approve) {
      XML.save(fileChooser.selectedFile.getCanonicalPath,
               Shape.serialize(ReShapes.drawingSpaceState.now.shapes.now))
      ReShapes.drawingSpaceState.now.fileName set fileChooser.selectedFile.getName
      ReShapes.ui.tabbedPane.pages(ReShapes.ui.tabbedPane.selection.index).title = fileChooser.selectedFile.getName
    }
  }
}

/**
 * Deserializes shapes from a chosen file
 */
class LoadAction extends Action("Load") {
  def apply() = {
    val fileChooser = new FileChooser()
    if (fileChooser.showDialog(null, "load") == FileChooser.Result.Approve) {
      ReShapes.drawingSpaceState.now.clear.fire()
      for (shape <- Shape.deserialize(XML.loadFile(fileChooser.selectedFile),
                                      ReShapes.drawingSpaceState.now))
        ReShapes.drawingSpaceState.now execute new CreateShape(shape)
    }
  }
}
