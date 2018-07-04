package rescala.crdts

import io.circe.generic.auto._
import loci.communicator.ws.akka.WS
import loci.registry.{Binding, BindingBuilder, Registry}
import loci.serializer.circe._
import loci.transmitter.{Marshallable, RemoteRef, Transmittable}
import rescala.crdts.pvars.Publishable._
import rescala.crdts.statecrdts.sets.ORSet

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
//import rescala.crdts.pvars.PGrowOnlyCounter._
//import rescala.crdts.pvars.PSet._
import rescala.crdts.pvars._
// import rescala.crdts.statecrdts.sets.ORSet

import scala.language.higherKinds

/*

object PVarTransmittable {
  implicit def rescalaSignalTransmittable[ValueType, CrdtType, S](implicit
                                                   transmittable: Transmittable[CrdtType, S, CrdtType],
                                                   serializable: Serializable[S], pVarFactory: PVarFactory[ValueType,CrdtType]) = {
    type From = CrdtType
    type To = CrdtType
    type P = Publishable[ValueType, CrdtType]

    new PushBasedTransmittable[P, From, S, To, P] {


      def send(value: P, remote: RemoteRef, endpoint: Endpoint[From, To]): To = {

        val observer = value.internalChanges.observe(c => endpoint.send(c))

        endpoint.receive notify value.externalChanges.fire

        endpoint.closed notify { _ => observer.remove }

        value.crdtSignal.readValueOnce
      }

      def receive(value: To, remote: RemoteRef, endpoint: Endpoint[From, To]): P = {
        val pvar: P = pVarFactory.create()
        locally(pvar.valueSignal)
        pvar.externalChanges.fire(value)

        println(s"received $value")
        println(s"before: $pvar, ")

        endpoint.receive notify pvar.externalChanges.fire
        val observer = pvar.internalChanges.observe(c => endpoint.send(c))
        endpoint.closed notify { _ => observer.remove }

        //println(s"manual ${implicitly[StateCRDT[ValueType, CrdtType]].merge(pvar.crdtSignal.readValueOnce, value)}")

        println(s"after: $pvar")

        pvar
      }
    }
  }
}

*/

//noinspection ScalaUnusedSymbol
object testSignalExpressions {
  def main(args: Array[String]): Unit = {

    //    // set up networking
    //    val config = ConfigFactory.parseString("akka.remote.netty.tcp.port=" + 2506).
    //      withFallback(ConfigFactory.load())
    //    val system: ActorSystem = ActorSystem("ClusterSystem", config)
    //    implicit val engine: ActorRef = system.actorOf(DistributionEngine.props("Host1", 1000), "Host1")

    //    val c1 = PGrowOnlyCounter()
    //    val c2 = PGrowOnlyCounter(1)
    //
    //    val sig = Signal {
    //      c1() + c2()
    //    }
    //
    //    println(sig.readValueOnce)
    //
    //
    //    val v1 = Var(List(0))
    //    val v2 = Var(List(1))
    //
    //    val sig3 = Signal {
    //      v1() ++ v2()
    //    }
    //    println(sig3)

    //import PVarTransmittable._
    //import PGrowOnlyCounter._
    //import GCountTransmittable._

    //val p:Publishable[_,_] = PGrowOnlyCounter()

    //    val counterBinding = Binding[PGrowOnlyCounter]("counter")(BindingBuilder.value[PGrowOnlyCounter])
    //     pSetTransmittableManual[Int, ORSet[Int]]

    val setBinding = Binding[PSet[Int]]("set")
    /* val counterBinding = Binding[PGrowOnlyCounter]("counter")(BindingBuilder.value[PGrowOnlyCounter](
      Marshallable.marshallable[GCounter, Int, GCounter](
        PGrowOnlyCounter.pGrowOnlyCounterTransmittable.asInstanceOf[Transmittable[GCounter,Int,GCounter]], implicitly[Serializable[Int]]
      ))
//    */
    println("running server")

    val (sr, s2) = { //server
      val registry = new Registry
      registry.listen(WS(1099))

      val l1 = PSet(Set(10))
      registry.bind(setBinding)(l1)

      (registry, l1)
    }

    println("running client")

    /**
      * { //client
      * val registry = new Registry
      * val connection: Future[RemoteRef] = registry.connect(WS("ws://localhost:1099/"))
      *connection.foreach { remote =>
      * val subscribedSig = registry.lookup(counterBinding, remote)
      * println("subscription")
      *subscribedSig.foreach(c => c.valueSignal.observe(v => println("client value: " + v)))
      * }
      *connection.failed.foreach(println)
      * *
      *
      * }
      **/

    val (cr, c1) = { //client2
      val registry = new Registry
      val connection: Future[RemoteRef] = registry.connect(WS("ws://localhost:1099/"))
      val remote: RemoteRef = Await.result(connection, Duration.Inf)
      val subscribedSig: Future[PSet[Int]] = registry.lookup(setBinding, remote)
      val set: PSet[Int] = Await.result(subscribedSig, Duration.Inf)
      (registry, set)
    }

    println("done")
    s2.add(20)
    c1.add(30)

    Thread.sleep(1000)
    Thread.sleep(1000)

    println("s2: " + s2.value)
    println("c1: " + c1.value)
    println("s2 == c1? " + (s2.value ==  c1.value))
    println("slept")
    sr.terminate()
  }
}

//
////noinspection ScalaUnusedSymbol
//object testVertices {
//  def main(args: Array[String]): Unit = {
//    val v1 = Vertex("System: Hello Alice")
//    val v2 = Vertex("[Alice]: Hey Bob!")
//    val v3 = Vertex("[Alice]: How are you?")
//
//
//    //val vset = Set(v1,v1)
//    Thread sleep 2000
//
//    val u1 = Vertex("System: Hello Bob")
//    val u2 = Vertex("[Bob]: Hey Alice!")
//    val u3 = Vertex("[Bob]: How is Eve doing?")
//
//    var r: RGOA[String] = RGOA()
//    r = r.append(v1)
//    r = r.append(v2)
//    r = r.addRight(v2, v3)
//
//    //var r3: RGOA[String] = RGOA.empty
//
//    var s:RGOA[String] = RGOA()
//    s = s.append(u1).append(u2).append(u3)
//
//    println(s.contains(u1))
//    println(s.containsValue("[Bob]: Hey Alice!"))
//
//    println(v1.timestamp < u1.timestamp)
//
//    var r2 = r.merge(s)
//    println(r2)
//    r2 = r2.merge(r)
//    println(r2)
//    //r2 = r2.addRight(v3, Vertex("[Bob]: I am fine! How about you?"))
//    //println(r)
//  }
//}
//
///*
//object testDistribution {
//  def main(args: Array[String]): Unit = {
//    val config = ConfigFactory.parseString("akka.remote.netty.tcp.port=" + 2560).
//      withFallback(ConfigFactory.load())
//
//    val system: ActorSystem = ActorSystem("ClusterSystem", config)
//    val host1: ActorRef = system.actorOf(DistributionEngine.props("Host1"), "Host1")
//    val host2: ActorRef = system.actorOf(DistributionEngine.props("Host2"), "Host2")
//
//    val c = DistributedGCounter(host1, "moppi", 12)
//    c.publish()
//    c.increase
//    c.publish()
//    println(c.value)
//    println()
//
//    val d = DistributedGCounter(host2, "moppi", 0)
//    d.publish()
//    val doubledMoppi = Signal {
//      c.signal().value + d.signal().value
//    }
//
//    println(s"doubledMoppi: ${doubledMoppi.readValueOnce}")
//    println(c.value)
//    println(d.value)
//    println()
//
//    Thread sleep 20000
//    d.increase
//
//    println(s"doubledMoppi: ${doubledMoppi.readValueOnce}")
//    println(c.value)
//    println(d.value)
//
//    system.terminate()
//  }
//}
//*/
//
///**
//  * Created by julian on 05.07.17.
//  */
//object CRDTsMainTest {
//  def main(args: Array[String]): Unit = {
//    val system: ActorSystem = ActorSystem("crdtTestbench")
//
//    val m1 = Vertex("[Alice] Hello Bob!")
//    var log = RGA[String]()
//    log = log.addRight(Vertex.start, m1)
//    val m2 = Vertex("[Bob] Hey Alice!")
//    val m3 = Vertex("[Alice] How are you?")
//    log = log addRight(m1, m2) addRight(m2, m3)
//    println(log.value)
//
//    log = log.remove(m2)
//    println(log.before(m1, m2))
//    println(log.before(m2, m1))
//    println(log.before(m1, m3))
//    println(log.value)
//
//    system.terminate()
//    //println(r.successor(start).value)
//    //println(r.before(start,end))
//    //println(r.before(end,start))
//    /*
//        val lookupServer: ActorRef = system.actorOf(LookupServer.props, "lookupServer")
//        val host1: ActorRef = system.actorOf(DistributionEngine.props("Host1", lookupServer), "Host1")
//        val host2: ActorRef = system.actorOf(DistributionEngine.props("Host2", lookupServer), "Host2")
//
//        val c = CCounter(host1, "moppi", 12)
//        c.increase
//        println(c.value)
//        println()
//
//        val d = CCounter(host2, "moppi", 0)
//        val doubledMoppi = Signal {
//          c.signal().value + d.signal().value
//        }
//
//    doubledMoppi.observe(v => println("Observed: " + v))
//    d.increase
//
//    println(s"doubledMoppi: ${doubledMoppi.readValueOnce}")
//    println(c.value)
//    println(d.value)
//    println()
//
//    //Thread sleep 10000
//    println(s"doubledMoppi: ${doubledMoppi.readValueOnce}")
//    println(c.value)
//    println(d.value)
//
//    println(r.vertices)
//    println(r.edges)
//    println(r.successor(start).value)
//    println(r.before(start, end))
//
//
//    system.terminate()
//
//    */
//
//
//    // ###### Old stuff ######
//
//    /*Engine.host = "Host1"
//
//val a = Var(CIncOnlyCounter(11))
//DistributionEngine.publish("moppi", a)
//
//DistributionEngine.host = "Host2"
//val b = Var(CIncOnlyCounter(13))
//DistributionEngine.publish("moppi", b)
//println(b.readValueOnce.payload)
//
//b.transform(_.increase)
//
//DistributionEngine.host = "Host1"
////a.set(a.readValueOnce.increase)
////b.set(b.readValueOnce.increase)
//println(a.readValueOnce)
//println(b.readValueOnce)
//*/
//
//    /*
//    val system: ActorSystem = ActorSystem("crdtTestbench")
//
//    val l: ActorRef = system.actorOf(LookupServer.props, "lookupServer")
//    val host1: ActorRef = system.actorOf(DistributionEngine.props("Host1", l), "Host1")
//    val host2: ActorRef = system.actorOf(DistributionEngine.props("Host2", l), "Host2")
//
//    val a = Var(CIncOnlyCounter(11))
//    host1 ! Publish("moppi", a)
//
//    val b = Var(CIncOnlyCounter(0))
//    host2 ! Publish("moppi", b)
//
//    b.transform(_.increase)
//    */
//
//
//    /*
//    var c = ORSet(1, 2, 3)
//    println(c)
//    var d = c
//    println(d)
//    c = c.remove(3)
//    d = d.add(3)
//    d = d merge c
//    println(d)
//    println(d.payload)
//    d = d.fromValue(d.value)
//    println(d.payload)
//    */
//    /*
//
//    This should work:
//
//    val counter: Signal[CIncOnlyCounter] =  e.fold(CIncOnlyCounter(13)) { (c, _) => c.increase }
//
//    val syncedCounter: Signal[CIncOnlyCounter] = DistributionEngine.publish("moppi", counter)
//
//
//    DistributionEngine.host = "Host2"
//    val otherCounter = DistributionEngine.subscribe(counter)
//
//    */
//
//
//    /**
//      *DistributionEngine.host = "Host3"
//      * val c = Var(CIncOnlyCounter(0))
//      *DistributionEngine.publish("moppi", c)
//      **/
//  }
//}
