package distributionengine

import java.net.InetAddress

import akka.actor._
import akka.cluster.pubsub.DistributedPubSubMediator._
import akka.cluster.pubsub._
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.scalalogging._
import rescala._
import statecrdts._

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.{Failure, Success}

/**
  * Handles distribution on the client side.
  */
class DistributionEngine(hostName: String = InetAddress.getLocalHost.getHostAddress, val pulseEvery: Long = 1000,
                         private var delayTime: Long = 0, private var online: Boolean = true) extends Actor {

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
    case PublishVar(varName, pVar) => publish(varName, pVar); sender ! subscribe(varName)
    case PublishReadOnly(varName, pVar) => publish(varName, pVar)
    case SubscribeVar(varName, evt) => extChangeEvts += varName -> evt; sender ! subscribe(varName)
    case UpdateMessage(varName, value, hostRef) =>
      delay()
      logger.debug(s"[$hostName] received value $value for $varName from ${hostRef.path.name}")
      val newHosts = registry(varName) + hostRef
      registry += (varName -> newHosts) // add sender to registry
      logger.debug(s"registry is now: $registry")
      // issue external change event
      extChangeEvts(varName)(value)
    case QueryMessage(varName, hostRef) =>
      delay()
      logger.debug(s"[$hostName] ${hostRef.path.name} queried variable $varName")
      broadcastUpdates(varName, pVars(varName).crdtSignal.now)
    case SyncAllMessage => pVars foreach {
      case (varName: String, pVar: Publishable[StateCRDT]) => broadcastUpdates(varName, pVar.crdtSignal.now)
    }
    case "tick" => pVars foreach { // pulse updates every second
      case (varName: String, pVar: Publishable[StateCRDT]) => mediator ! Publish(varName, UpdateMessage(varName, pVar.crdtSignal.now, self))
    }
    case SetDelay(time: Long) => delayTime = time
  }

  def delay(): Unit = Thread sleep delayTime

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
    logger.debug(s"[$hostName] Sending updates for $varName to ${registry(varName)}")
    /*
    registry(varName).foreach(a => {
      a ! UpdateMessage(varName, value.payload, self)
    })
    */

    mediator ! Publish(varName, UpdateMessage(varName, value, self))
  }

  def subscribe(varName: String): Int = {
    implicit val timeout = Timeout(20.second)

    var returnValue = 1
    mediator ! Publish(varName, QueryMessage(varName, self))
    // Query value from all subscribers. This will also update our registry of other hosts.
    val sendMessage = mediator ? Subscribe(varName, self) // register this instance
    val registration = sendMessage andThen {
      case Success(m) => m match {
        case SubscribeAck(Subscribe(`varName`, None, `self`)) =>
          // set return value to 0 if everything was successful
          returnValue = 0
      }
      case Failure(_) => logger.debug("[" + hostName + "] Could not reach lookup server!")
    }
    Await.ready(registration, Duration.Inf) // await the answer of the lookupServer

    query(varName) // query for updates

    returnValue
  }

  /**
    * Query for updates of a variable.
    *
    * @param varName Name of the queried variable
    */
  private def query(varName: String): Unit = {
    mediator ! Publish(varName, QueryMessage(varName, self))
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
  def props(host: String): Props = Props(new DistributionEngine(host))

  def props(host: String, pulseEvery: Long): Props = Props(new DistributionEngine(host, pulseEvery))

  def ip: Identifier = InetAddress.getLocalHost.getHostAddress

  /**
    * Generates unique identifiers based on the current Hostname, IP address and a UUID based on the current system time.
    *
    * @return A new unique identifier (e.g. hostname/127.0.0.1::1274f9fe-cdf7-3f10-a7a4-33e8062d7435)
    */
  def genId: String = host + "::" + java.util.UUID.nameUUIDFromBytes(BigInt(System.currentTimeMillis).toByteArray)

  def host: InetAddress = InetAddress.getLocalHost // hostname + IP
}