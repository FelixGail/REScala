package rescala.crdts.pvars

import java.net.InetAddress

import akka.actor._
import akka.cluster.pubsub.DistributedPubSubMediator._
import akka.cluster.pubsub._
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.scalalogging._
import rescala.crdts.pvars.DistributionEngine._
import rescala._
import rescala.crdts.statecrdts._

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.{Failure, Success}

/**
  * Handles distribution on the client side.
  */
class DistributionEngine(hostName: String = InetAddress.getLocalHost.getHostAddress, val pulseEvery: Long = 1000,
                         private var delayTime: Time = 0, private var online: Boolean = true) extends Actor {

  if (pulseEvery > 0) { //noinspection ScalaUnusedSymbol
    // update from time to time, if pulse is set
    val tick = context.system.scheduler.schedule(500 millis, pulseEvery millis, self, "tick")
  }

  private val logger: Logger = Logger[DistributionEngine]
  private val mediator: ActorRef = DistributedPubSub.get(context.system).mediator
  private var pVars: Map[String, Publishable[_ <: StateCRDT]] = Map()
  private var extChangeEvts: Map[String, Evt[StateCRDT]] = Map()
  private var registry: Map[String, Set[ActorRef]] = Map().withDefaultValue(Set())

  def receive: PartialFunction[Any, Unit] = {
    case SetOnline(o: Boolean) => online = o
    case SetDelay(time: Time) => delayTime = time
    case message => // receive these messages only when online
      if (online) message match {
        case PublishVar(varName, pVar) => publish(varName, pVar); sender ! subscribe(varName)
        case PublishReadOnly(varName, pVar) => publish(varName, pVar)
        case SubscribeVar(varName, evt) => extChangeEvts += varName -> evt; subscribe(varName)
        case UpdateMessage(varName, value, hostRef) =>
          if (hostRef != self){
            context.system.scheduler.scheduleOnce(delayTime milliseconds) { // delay messages if delay set
              logger.debug(s"[$hostName] received value $value for $varName from ${hostRef.path.name}")
              val newHosts = registry(varName) + hostRef
              registry += (varName -> newHosts) // add sender to registry
              logger.debug(s"registry is now: $registry")
              // issue external change event
              extChangeEvts(varName)(value)
            }
          }
        case QueryMessage(varName, hostRef) =>
          context.system.scheduler.scheduleOnce(delayTime milliseconds) { // delay messages if delay set
            logger.debug(s"[$hostName] ${hostRef.path.name} queried variable $varName")
            broadcastUpdates(varName, pVars(varName).crdtSignal.now)
          }
        case SyncAllMessage => pVars foreach {
          case (varName: String, pVar: Publishable[StateCRDT]) => broadcastUpdates(varName, pVar.crdtSignal.now)
        }
        case "tick" => pVars foreach { // pulse updates every second
          case (varName: String, pVar: Publishable[StateCRDT]) => mediator ! Publish(varName, UpdateMessage(varName, pVar.crdtSignal.now, self))
        }
      }
  }

  /**
    * Publishes a variable on this host to the other hosts under a given name.
    *
    * @param pVar the local pVar to be published
    */
  def publish(varName: String, pVar: Publishable[_ <: StateCRDT]): Unit = {
    if (!pVars.contains(varName)) { // only publish if this hasn't been published before
      logger.info(s"Publishing $varName")
      // save reference to local var
      pVars += (varName -> pVar)
      // save external changeEvt for convenience
      extChangeEvts += (varName -> pVar.externalChanges.asInstanceOf[Evt[StateCRDT]])

      // add listener for internal changes
      pVar.internalChanges += (newValue => {
        logger.debug(s"[$hostName] Recognized internal change on $varName. $varName is now $newValue")
        broadcastUpdates(varName, newValue)
      })
    }

    // Update all other hosts
    broadcastUpdates(varName, pVar.crdtSignal.now)
  }

  private def broadcastUpdates(varName: String, value: StateCRDT): Unit = {
    if (online){
      logger.debug(s"[$hostName] Sending updates for $varName to ${registry(varName)}")
      /*
      registry(varName).foreach(a => {
        a ! UpdateMessage(varName, value.payload, self)
      })
      */

      mediator ! Publish(varName, UpdateMessage(varName, value, self))
    }
  }

  def subscribe(varName: String): Unit = {
    implicit val timeout = Timeout(20.second)

    mediator ! Publish(varName, QueryMessage(varName, self))
    // Query value from all subscribers. This will also update our registry of other hosts.
    val sendMessage = mediator ? Subscribe(varName, self) // register this instance
    val registration = sendMessage andThen {
      case Success(m) =>
      case Failure(_) => logger.debug("[" + hostName + "] Could not reach lookup server!")
    }
    Await.ready(registration, Duration.Inf) // await the answer of the lookupServer

    query(varName) // query for updates
  }

  /**
    * Query for updates of a variable.
    *
    * @param varName Name of the queried variable
    */
  private def query(varName: String): Unit = {
    if (online) mediator ! Publish(varName, QueryMessage(varName, self))
  }

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
  // types
  type Time = Long

  // message types
  case class PublishVar(varName: String, pVar: Publishable[_ <: StateCRDT])

  case class PublishReadOnly(varName: String, pVar: Publishable[_ <: StateCRDT])

  case class SubscribeVar(varName: String, event: Evt[StateCRDT])

  case class SyncVar(cVar: Publishable[_ <: StateCRDT])

  case class SyncAllMessage()

  case class UpdateMessage(varName: String, crdt: StateCRDT, hostRef: ActorRef)

  case class QueryMessage(varName: String, host: ActorRef)

  case class RegisterMessage(varName: String, host: ActorRef)

  case class SetDelay(time: Time)

  case class SetOnline(online: Boolean)

  def props(hostName: String) = Props(new DistributionEngine(hostName))

  def props(host: String, pulseEvery: Long): Props = Props(new DistributionEngine(host, pulseEvery))

  def host: InetAddress = InetAddress.getLocalHost // hostname + IP

  def ip: String = InetAddress.getLocalHost.getHostAddress

  def subscribe[A](name: String, initial: A)(implicit engine: ActorRef): Signal[A] = subscribe(name).map {
    case None => initial
    case a: A => a
  }

  def subscribe(name: String)(implicit engine: ActorRef): Signal[Any] = {
    val evt = Evt[StateCRDT]
    engine ! SubscribeVar(name, evt)
    evt.latestOption().map {
      // return latest crdt or None if we didn't receive a value yet
      case Some(crdt) => crdt.value
      case _ => None
    }
  }

  def setDelay(time: Time)(implicit engine: ActorRef): Unit = engine ! SetDelay(time)

  def setOnline(online: Boolean)(implicit engine: ActorRef): Unit = engine ! SetOnline(online)
}
