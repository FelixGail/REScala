package tests.rescala

import rescala.core.Engine

sealed trait ChangeX
case object DontSet extends ChangeX
case object SetUnchanged extends ChangeX
case object SetChanged extends ChangeX

class EvaluationOrderWithHigherOrderSignalsTest extends RETests {
  def run(engine: Engine[TestStruct], changeX: ChangeX): Unit = {
    import engine._

    val initialX = "initialValue"
    val newX = if (changeX == SetChanged) "changedValue" else initialX

    val results = for(i <- 0 to 10) yield {

      val x = Var(initialX)
      val x4 = x.map(identity).map(identity).map(identity).map(identity)

      val ho = Var(x: Signal[String])
      var reevaluationRestartTracker = List.empty[String]
      val flatten = Signal.dynamic {
        val res = ho()()
        reevaluationRestartTracker ::= res
        res
      }

      changeX match {
        case DontSet => ho.set(x4)
        case _ => update(x -> newX, ho -> x4)
      }

      // final value should be correct
      assert(flatten.now == newX)
      // value should be determined by reevaluating twice after discovering a higher-level dependency on first run
      reevaluationRestartTracker
    }

    results.foreach{ r =>
      assert(r.dropWhile(_ == newX).dropWhile(_ == initialX) == List())
    }
  }

  allEngines("dont set")(run(_, DontSet))
  allEngines("set unchanged")(run(_, SetUnchanged))
  allEngines("set changed")(run(_, SetChanged))
}
