package daimpl.simpletodo

import scala.scalajs.js.JSApp
import scalatags.JsDom.all._
import rescala._
import rescalatags._
import org.scalajs.dom
import dom.document

object SimpleTodo extends JSApp {
  var unique = 0

  class Task(desc_ : String, done_ : Boolean) {
    val id   = unique
    val desc = Var(desc_)
    val done = Var(done_)
    unique += 1
  }

  def main(): Unit = {

    val tasks = Var(List(
      new Task("get milk", false),
      new Task("get sugar", false),
      new Task("get coffee", false),
      new Task("walk the dog", false)
    ))

    document.body.appendChild(div(
      h1("DO TODOS!"),

      form(
        `class`:="task",
        onsubmit:= { e: dom.UIEvent =>
          e.preventDefault()

          val input = document.getElementById("newtodo")
            .asInstanceOf[dom.html.Input]
          tasks() = new Task(input.value, false) :: tasks.now
          input.value = ""
        },
        span(`class`:="span-input"),
        input(`class`:="descrip", id:="newtodo", placeholder:="new todo")
      ),

      Signal {
        div(
          `class`:= Signal { if (tasks().size == 0) "info" else "hidden"},
          "All clear"
        )
      }.asFrag,

      Signal { ul(tasks().map { t =>
        li(

          // TODO why does this work, implicit function?
          `class`:= Signal{ if (t.done()) "task done" else "task" } ,

          Signal { input(
            `type`:="checkbox",

            // TODO ? use attrValue / .asAttr
            if (t.done()) checked:="checked" else "",

            onchange:={ e: dom.UIEvent =>
              t.done() = e.target.asInstanceOf[dom.html.Input].checked
            }
          ) }.asFrag,

          // bidirectional binding only with onchange, not with oninput :(
          input(
            value := t.desc(),
            onchange := { e: dom.UIEvent =>
              t.desc() = e.target.asInstanceOf[dom.html.Input].value
//              tasks() = tasks.now.filter { (x)=> x.desc.now != "" }
            }
          )
        )
      }) }.asFrag,

      input(
        `type`:="button",
        `class`:=Signal { if (tasks().size==0) "hidden" else ""},
        value:="remove all done todos", onclick:={ e: dom.UIEvent =>
          tasks() = tasks.now.filter { t => !t.done.now }
        }
      )
    ).render)
  }
}
