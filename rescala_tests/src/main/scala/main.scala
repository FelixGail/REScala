import akka.actor.ActorRef
import rescala._
import stateCrdts._
import scala.concurrent.duration._
import akka.pattern.ask
import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.util.Timeout
import stateCrdts.DistributionEngine.Publish

import scala.collection.immutable.HashMap

/**
  * Created by julian on 05.07.17.
  */
object main {
  def main(args: Array[String]): Unit = {
    /*Engine.host = "Host1"

    val a = Var(CIncOnlyCounter(11))
    DistributionEngine.publish("moppi", a)

    DistributionEngine.host = "Host2"
    val b = Var(CIncOnlyCounter(13))
    DistributionEngine.publish("moppi", b)
    println(b.now.payload)

    b.transform(_.increase)

    DistributionEngine.host = "Host1"
    //a.set(a.now.increase)
    //b.set(b.now.increase)
    println(a.now)
    println(b.now)
    */
    val system: ActorSystem = ActorSystem("crdtTestbench")

    val l: ActorRef = system.actorOf(LookupServer.props, "lookupServer")
    val host1: ActorRef = system.actorOf(DistributionEngine.props("Host1", l), "Host1")
    val host2: ActorRef = system.actorOf(DistributionEngine.props("Host2", l), "Host2")

    val a = Var(CIncOnlyCounter(11))
    host1 ! Publish("moppi", a)

    val b = Var(CIncOnlyCounter(0))
    host2 ! Publish("moppi", b)

    b.transform(_.increase)

    /*
    var c = ORSet(1, 2, 3)
    println(c)
    var d = c
    println(d)
    c = c.remove(3)
    d = d.add(3)
    d = d merge c
    println(d)
    println(d.payload)
    d = d.fromValue(d.value)
    println(d.payload)
    */
    /*

    This should work:

    val counter: Signal[CIncOnlyCounter] =  e.fold(CIncOnlyCounter(13)) { (c, _) => c.increase }

    val syncedCounter: Signal[CIncOnlyCounter] = DistributionEngine.publish("moppi", counter)


    DistributionEngine.host = "Host2"
    val otherCounter = DistributionEngine.subscribe(counter)

    */


    /**
    DistributionEngine.host = "Host3"
    val c = Var(CIncOnlyCounter(0))
    DistributionEngine.publish("moppi", c)
      **/
  }
}
