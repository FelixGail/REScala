package tests.rescala.misc

import tests.rescala.testtools.RETests

import scala.language.implicitConversions

class LightImplicitSyntaxTest extends RETests { multiEngined { engine => import engine._

  test("experiment With Implicit Syntax") {

    implicit def getSignalValueDynamic[T](s: Signal[T])(implicit ticket: engine.DynamicTicket): T = ticket.depend(s)
    def Signal[T](f: DynamicTicket => T)(implicit maybe: CreationTicket): Signal[T] = dynamic()(f)

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

} }
