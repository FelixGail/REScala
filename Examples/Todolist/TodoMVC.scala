package daimpl.todomvc

import scala.scalajs.js.JSApp
import scalatags.JsDom.all._
import scalatags.JsDom.tags2.section
import rescalatags._
import rescala.LocalStorageStore
import org.scalajs.dom
import org.scalajs.dom.html.Input
import org.scalajs.dom.{UIEvent, document}
import rescala.core.ReCirce.recirce
import rescala.core.ReSerializable

object TodoMVC extends JSApp {

  implicit val storingEngine: LocalStorageStore = new LocalStorageStore()
  import storingEngine._

//  var unique = 0

  implicit val taskDecoder: io.circe.Decoder[Task] = io.circe.Decoder.forProduct3[String, Boolean, List[String], Task]("decs", "done", "names") { (dec, don, names) =>
    storingEngine.addNextNames(names: _*)
    new Task(dec, don)
  }
  implicit val taskEncoder: io.circe.Encoder[Task] = io.circe.Encoder.forProduct3[String, Boolean, List[String], Task]("decs", "done", "names"){t =>
    (t.desc.now, t.done.now, List(getName(t.desc), getName(t.done)))
  }

  class Task(desc_ : String, done_ : Boolean) {
//    val id   = unique
    val desc = Var(desc_)
    val done = Var(done_)
    val editing = Var(false)(ReSerializable.doNotSerialize, implicitly)
//    unique += 1
  }

  def main(): Unit = {

    val innerTasks = List(
      new Task("get milk", false),
      new Task("get sugar", false),
      new Task("get coffee", false),
      new Task("walk the dog", false)
    )
    storingEngine.addNextNames("tasklist")
    val tasks = Var(innerTasks)

    lazy val newTodo: Input = input(
      id := "newtodo",
      `class` := "new-todo",
      placeholder := "What needs to be done?",
      autofocus := "",

      // TODO onchange --> on enter
      onchange := { e: UIEvent =>
        e.preventDefault()
        tasks.transform(new Task(newTodo.value, false) :: _)
        newTodo.value = ""
      }
    ).render

    document.body.replaceChild(div(
      `class`:="todoapp",
      header(
        `class`:="header",
        h1("todos"),
        newTodo
      ),

      section(
        `class`:= "main",
        `style`:= Signal { if(tasks().isEmpty) "display:hidden" else "" },
        input( `class`:="toggle-all", `type`:="checkbox",
//          checked:=tasks.now.map { it => it.done.now }
//                   .reduce {  (a, b) => a && b},
          onclick:={ e: dom.UIEvent =>
            println("eh?")
            tasks.now.foreach { it =>
              it.done() = !e.target.asInstanceOf[dom.html.Input].checked } }),
        label(`for`:="toggle-all", "Mark all as complete"),
        Signal { ul(`class`:="todo-list", tasks().map { t =>

          li(
            `class`:=
               (if (t.done()) "completed " else "")
              +(if (t.editing()) "editing " else ""),

            div(
              `class`:="view",

              ondblclick:={ e: dom.UIEvent => t.editing() = true },

              input( `class`:="toggle", `type`:="checkbox",
                if (t.done()) checked else "",
                onchange:={ e: dom.UIEvent =>
                  t.done() = e.target.asInstanceOf[dom.html.Input].checked }
              ),

              label(t.desc()),

              button(`class`:="destroy",
                onclick:= { e: dom.UIEvent =>
                  tasks.transform(_.filter { it => it != t })
                })
            ),

            input( `class`:="edit", `type`:="text",
              value:=t.desc(),
              onchange:={ e: dom.UIEvent =>
                val input = e.target.asInstanceOf[dom.html.Input]
                t.editing() = false
                t.desc() = input.value
              }
            )
          )

        }) }.asFrag
      ),

//            onchange := { e: dom.UIEvent =>
//              t.desc() = e.target.asInstanceOf[dom.html.Input].value
//              tasks() = tasks.now.filter { (x)=> x.desc.now != "" }
//            }

      input(
        `type`:="button",
        `class`:=Signal { if (tasks().size==0) "hidden" else ""},
        value:="remove all done todos", onclick:={ e: dom.UIEvent =>
          tasks() = tasks.now.filter { t => !t.done.now }
        }
      )
    ).render, document.body.firstElementChild)
  }
}

// NOTE that setting a Var inside a Signal leads to the following warning:
//   Statement may either be unnecessary or have side effects.
//   Signal expressions should have no side effects.
// This warning is not executed, but a function is defined.
// Which happens alot here, for button onclicks handlers.
