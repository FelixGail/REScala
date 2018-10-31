package live

import java.util.concurrent.ThreadLocalRandom

import org.scalajs.dom
import org.scalajs.dom.html.Input
import org.scalajs.dom.{UIEvent, document}
import rescala.core.{CreationTicket, ReSerializable}
import rescala.restoration.{LocalStorageStore, ReCirce}
import rescala.restoration.ReCirce.{recirce, varDecoder, varEncoder}
import rescala.debuggable.ChromeDebuggerInterface
import rescalatags._
import scalatags.JsDom.all._
import scalatags.JsDom.tags2.section

import scala.scalajs.js.annotation.JSExportTopLevel

object Demo {

  implicit val storingEngine: LocalStorageStore = new LocalStorageStore()
  import storingEngine._

  @JSExportTopLevel("live.Demo.main")
  def main(): Unit = {
    ChromeDebuggerInterface.setup(storingEngine)

    val content = storingEngine.transaction(){ implicit tx =>

      val temperature = Evt[Double]()("temperature")
      storingEngine.registerSource(temperature)

      val time = temperature.count()("time", implicitly)

      val filtered = temperature.filter(_ < 100)("less than 100").filter(_ >= -40)("more than -40")

      val history: Signal[Seq[Double]] = filtered.last(5)("history", implicitly)

      val aggregations = Map(
        "average" -> { x: Seq[Double] => x.sum / x.size },
        "max" -> { x: Seq[Double] => x.max },
        "sum" -> { x: Seq[Double] => x.sum }
      )

      val selectedAggregation = Var("sum")(implicitly, "selected aggregation")
      storingEngine.registerSource(selectedAggregation, aggregations.keys.toSeq: _*)

      val aggregation: Signal[Seq[Double] => Double] = Signal(aggregations(selectedAggregation.value))("aggregation")

      val aggregated = Signal {
        aggregation.value(history.value)
      }("aggregated")

      div(
        `class` := "todoapp",
        header(
          `class` := "header",
          h1("Temperatures"),
          Signal { span(s"time ${time.value}") }.asFrag
        ),

        section(
          `class` := "main",
          `style` := Signal {if (history.value.isEmpty) "display:hidden" else ""},
          Signal.dynamic {
            ul(`class` := "todo-list",
               history().map { temp =>
                 li(`class` := "no-editing ",
                    div(
                      `class` := "view",
                      label(temp),
                      ),
                    )
               })
          }.asFrag
        ),

        div(
          `class` := "footer",
          `style` := Signal {if (history.value.isEmpty) "display:none" else ""},

          label("aggregated: "),
          Signal {span(aggregated.value)}.asFrag
        )
      )
    }

    document.body.replaceChild(content.render, document.body.firstElementChild)

    ChromeDebuggerInterface.finishedLoading()
  }
}
