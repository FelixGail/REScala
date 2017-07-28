package distributionengine

import java.net.InetAddress

import akka.actor._
import akka.cluster.pubsub.DistributedPubSubMediator.{Publish, Subscribe, SubscribeAck}
import akka.cluster.pubsub._
import akka.pattern.ask
import akka.util.Timeout
import rescala._
import statecrdts._

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.{Failure, Success}

/**
  * Handles distribution on the client side and provides a frontend for the programmer to interact with.
  */
class DistributionEngine(hostName: String = InetAddress.getLocalHost.getHostAddress) extends Actor {
  val mediator: ActorRef = DistributedPubSub(context.system).mediator

  private var registry: Map[String, Set[ActorRef]] = Map().withDefaultValue(Set())
  var localVars: Map[String, Var[StateCRDT]] = Map()
  var localCVars: Map[String, Publishable[_ <: StateCRDT]] = Map()
  var extChangeEvts: Map[String, Evt[StateCRDT]] = Map()

  // fakes the existence of a server infrastructure
  // maps hostname to map of varName and value
  // TODO: make private
  //var cloudStorage: Map[String, Map[String, Any]] = Map().withDefaultValue(Map())

  def receive: PartialFunction[Any, Unit] = {
    case PublishVar(cVar) => sender ! publish(cVar)
    case SyncVar(cVar) => ??? // TODO: implement blocking sync operation, maybe with garbage collection
    case UpdateMessage(varName, value, hostRef) =>
      sleep()
      println(s"[$hostName] received value $value for $varName from ${hostRef.path.name}")
      localCVars(varName).externalChanges.asInstanceOf[Evt[StateCRDT]](value) // issue external change event
      val newHosts = registry(varName) + hostRef
      registry += (varName -> newHosts) // add sender to registry
      println(s"registry is now: $registry")
    case QueryMessage(varName, hostRef) =>
      sleep()
      println(s"[$hostName] ${hostRef.path.name} queried variable $varName")
      hostRef ! UpdateMessage(varName, localCVars(varName).signal.now, self) // send reply
    case SyncAllMessage => localCVars.foreach {
      case (varName: String, dVar: Publishable[StateCRDT]) => sendUpdates(varName, dVar.signal.now)
    }
  }

  /**
    * Publishes a variable on this host to the other hosts under a given name.
    *
    * @param dVar the local dVar to be published
    */
  def publish(dVar: Publishable[_ <: StateCRDT]): Int = {
    val varName = dVar.name
    // LookupServer Registration:
    implicit val timeout = Timeout(20.second)
    var returnValue: Int = 1

    // Query value from all subscribers. This will also update our registry of other hosts.
    mediator ! Publish(varName, QueryMessage(varName, self))
    val sendMessage = mediator ? Subscribe(varName, self) // register this instance

    val registration = sendMessage andThen {
      case Success(m) => m match {
        case SubscribeAck(Subscribe(`varName`, None, `self`)) =>
          // save reference to local var
          localCVars += (dVar.name -> dVar)

          // add listener for internal changes
          dVar.internalChanges += (newValue => {
            println(s"[$hostName] Recognized internal change on ${dVar.name}. ${dVar.name} is now $newValue")
            sendUpdates(varName, newValue)
          })

          // set return value to 0 if everything was successful
          returnValue = 0
      }
      case Failure(_) => println("[" + hostName + "] Could not reach lookup server!")
    }

    Await.ready(registration, Duration.Inf) // await the answer of the lookupServer

    // Update all other hosts
    sendUpdates(varName, dVar.signal.now)

    returnValue
  }

  private def sendUpdates(varName: String, value: StateCRDT): Unit = {
    println(s"[$hostName] Sending updates. Registry is $registry")
    registry(varName).foreach(a => {
      println(s"[$hostName] Sending update message to ${a.path.name}")
      a ! UpdateMessage(varName, value, self)
    })
  }

  private def query(varName: String): Unit = {
    registry(varName).foreach(a => a ! QueryMessage(varName, self))
  }

  /*
  /**
    * Updates the value of a given variable on all hosts.
    *
    * @param varName The public name of the published variable
    * @param value   The new value for the published variable
    */
  def set(varName: String, value: StateCRDT): Unit =
    registry(varName).filter((hostname) => hostname != host) // get all hosts differing from this one
      .foreach((hostname) => {
      cloudStorage = cloudStorage +
        (hostname -> (cloudStorage(hostname) +
          (varName -> value.payload))) // add new value to cloudStorage

      val localVar = localVars(hostname)(varName).asInstanceOf[Var[StateCRDT]]
      val localPayload = localVar.now.payload
      if (localVar.now.payload != value.payload) { // only update local instance if payload changed
        localVar.set(localVar.now.merge(value))
        println("[" + hostname + "] Setting " + varName + "(" + localVar.now.asInstanceOf[CIncOnlyCounter].id + ") from " + localPayload + " to " + value.payload)
      }
      //Thread sleep 2000
    })
    */

  /**
    * Pull all hosts and return the new merged value for a given name
    */
  /* TODO: implement or remove
  def update(varName: String, localVal: StateCRDT) =
    registry(varName).foldLeft(localVal)((newValue, hostname) =>
      newValue.merge(newValue.fromPayload(cloudStorage(hostname)(varName).asInstanceOf[newValue.payloadType])).asInstanceOf[StateCRDT])
  */
}

object DistributionEngine {
  def props(host: String): Props = Props(new DistributionEngine(host))

  def host: InetAddress = InetAddress.getLocalHost // hostname + IP
  def ip: Identifier = InetAddress.getLocalHost.getHostAddress

  /**
    * Generates unique identifiers based on the current Hostname, IP address and a UUID based on the current system time.
    *
    * @return A new unique identifier (e.g. hostname/127.0.0.1::1274f9fe-cdf7-3f10-a7a4-33e8062d7435)
    */
  def genId: String = host + "::" + java.util.UUID.nameUUIDFromBytes(BigInt(System.currentTimeMillis).toByteArray)
}