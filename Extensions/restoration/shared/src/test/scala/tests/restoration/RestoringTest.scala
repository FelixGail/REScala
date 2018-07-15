package tests.restoration

import org.scalatest.FunSuite
import rescala.restoration.ReCirce._
import rescala.restoration.{InMemoryStore, RestoringInterface}

class RestoringTest extends FunSuite {

  def makeGraph()(implicit api: InMemoryStore) = {
    val e = api.Evt[Unit]()
    (e, e.count())
  }

  test("simple save and restore"){



    val snapshot = {
      implicit val api = RestoringInterface()
      val (e,c) = makeGraph()

      assert(c.readValueOnce == 0)

      e.fire()
      e.fire()

      assert(c.readValueOnce == 2)
      api.snapshot()
    }

    {
      implicit val engine1 = RestoringInterface(restoreFrom = snapshot)
      val (e,c) = makeGraph()

      assert(c.readValueOnce == 2)

      e.fire()

      assert(c.readValueOnce == 3)
    }
  }


  test("save and restore with changes in between"){

    val snapshot = {
      implicit val engine = RestoringInterface()
      val (e,c) = makeGraph()

      assert(c.readValueOnce == 0)

      e.fire()
      e.fire()

      assert(c.readValueOnce == 2)

      val mapped = c.map(_ + 10)("mapped")

      assert(mapped.readValueOnce == 12)

      engine.snapshot()

    }

    {
      implicit val engine1 = RestoringInterface(restoreFrom = snapshot)
      val (e,c) = makeGraph()

      assert(c.readValueOnce == 2)

      e.fire()

      assert(c.readValueOnce == 3)

      val mapped = c.map(_ + 10)("mapped")

      assert(mapped.readValueOnce == 13)


    }
  }

}
