package tests.restoration

import org.scalatest.FunSuite
import rescala.restoration.ReStoringScheduler
import rescala.restoration.ReCirce._

class RestoringTest extends FunSuite {

  test("simple save and restore"){

    val snapshot = {
      implicit val engine = new ReStoringScheduler()
      val e = engine.Evt[Unit]()
      val c = e.count()

      assert(c.now == 0)

      e.fire()
      e.fire()

      assert(c.now == 2)
      engine.snapshot()
    }

    {
      implicit val engine1 = new ReStoringScheduler(restoreFrom = snapshot)
      val e = engine1.Evt[Unit]()
      val c = e.count()

      assert(c.now == 2)

      e.fire()

      assert(c.now == 3)
    }
  }


  test("save and restore with changes in between"){

    val snapshot = {
      implicit val engine = new ReStoringScheduler()
      val e = engine.Evt[Unit]()
      val c = e.count()

      assert(c.now == 0)

      e.fire()
      e.fire()

      assert(c.now == 2)

      val mapped = c.map(_ + 10)

      assert(mapped.now == 12)

      engine.snapshot()

    }

    {
      implicit val engine1 = new ReStoringScheduler(restoreFrom = snapshot)
      val e = engine1.Evt[Unit]()
      val c = e.count()

      assert(c.now == 2)

      e.fire()

      assert(c.now == 3)

      val mapped = c.map(_ + 10)

      assert(mapped.now == 13)


    }
  }

}
