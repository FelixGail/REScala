package tests.rescala

import scala.language.implicitConversions

class LightImplicitSyntaxTest extends RETests {

  allEngines("experiment With Implicit Syntax") { engine =>
    import engine._

    implicit def getSignalValueDynamic[T](s: Signal[T])(implicit turn: engine.Turn): T = turn.depend(s)
    def Signal[T](f: Turn => T)(implicit maybe: Ticket): Signal[T] = dynamic()(f)

    val price = Var(3)
    val tax = price.map { p => p / 3 }
    val quantity = Var(1)
    val total = Signal { implicit t =>
      quantity * (price + tax)
    }

    assert(total.now === 4)
    price.set(6)
    assert(total.now === 8)
    quantity.set(2)
    assert(total.now === 16)

  }

}
