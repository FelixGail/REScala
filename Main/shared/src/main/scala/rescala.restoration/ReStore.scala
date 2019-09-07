package rescala.restoration

import rescala.core.Initializer.InitValues
import rescala.core._
import rescala.debuggable.{DebuggerInterface, DisableDebugging}
import rescala.interface.RescalaInterfaceRequireSerializer
import rescala.levelbased.{LevelBasedTransaction, LevelState, LevelStruct}
import rescala.twoversion.TwoVersionScheduler

import scala.collection.mutable

object RestoringInterface {
  def apply(restoreFrom: mutable.Map[REName, String] = mutable.HashMap()): InMemoryStore =
    new InMemoryStore(restoreFrom)
}


class ReStoringTurn(restore: ReStore, debuggerInterface: DebuggerInterface = DisableDebugging)
  extends LevelBasedTransaction[ReStoringStruct] {

  override protected def makeDerivedStructState[P](valuePersistency: InitValues[P],
                                                   creationTicket: CreationTicket[ReStoringStruct])
  : ReStoringState[P, ReStoringStruct] = {

    val nodeID = restore.makeNameUnique(creationTicket.rename)

    valuePersistency match {
      case is@Initializer.InitializedSignal(init) if is.serializable != rescala.core.ReSerializable.doNotSerialize =>
        if (is.serializable == rescala.core.ReSerializable.noSerializer)
          throw new Exception(s"restore requires serializable reactive: $valuePersistency")
        restore.get(nodeID) match {
          case None =>
            //println(s"new struct $name")
            restore.put(nodeID, is.serializable.serialize(valuePersistency.initialValue))
            new ReStoringState[P, ReStoringStruct](restore, nodeID, is.serializable, is)
          case Some(v) =>
            //println(s"old struct $name $s")
            val restoredValue = Initializer.InitializedSignal(is.serializable.deserialize(v).get)(is.serializable)
            new ReStoringState[P, ReStoringStruct](restore, nodeID, is.serializable, restoredValue)
        }
      case _ =>
        new ReStoringState(null, nodeID, null, valuePersistency)
    }
  }

  override protected[this] def register(reactive: ReSource[ReStoringStruct]): Unit = {
    restore.registerResource(reactive)
  }
  override def dynamicDependencyInteraction(dependency: ReSource[ReStoringStruct]): Unit = ()
  override def releasePhase(): Unit = ()
  override def preparationPhase(initialWrites: Set[ReSource[ReStoringStruct]]): Unit = ()
}

class ReStoringState[P, S <: Struct](storage: ReStore,
                                     val nodeID: REName,
                                     serializable: ReSerializable[P],
                                     initialVal: InitValues[P])
  extends LevelState[P, S](initialVal) {
  override def commit(): Unit = {
    super.commit()
    if (storage != null) {
      storage.put(nodeID, serializable.serialize(current))
    }
  }
}


trait ReStoringStruct extends LevelStruct {
  override type State[P, S <: Struct] = ReStoringState[P, S]
}

trait ReStore {
  def commitCurrentSnapshot(): String = ""
  def restoreSnap(snapId: String): Unit = ()

  def makeNameUnique(name: REName): REName
  def put(key: REName, value: String): Unit
  def get(key: REName): Option[String]
  def registerResource(r: ReSource[ReStoringStruct]): Unit
}

trait ReStoreImpl extends ReStore with TwoVersionScheduler[ReStoringStruct, ReStoringTurn] {


  var seenNames = Map[REName, Int]()
  var registeredNodes = Map[String, ReSource[ReStoringStruct]]()


  override def registerResource(r: ReSource[ReStoringStruct]): Unit = {
    registeredNodes += (r.state.nodeID.str -> r)
  }

  // disabled for now …
  def makeNameUnique(name: REName): REName = synchronized {
    name
//    val count =  seenNames.getOrElse(name, 0)
//    seenNames = seenNames.updated(name, count + 1)
//    if (count != 0) name.derive(count.toString) else name
  }

}


class InMemoryStore(restoreFrom: mutable.Map[REName, String])
  extends ReStoreImpl with RescalaInterfaceRequireSerializer[ReStoringStruct] {

  override def scheduler: Scheduler[ReStoringStruct] = this

  def values: mutable.Map[REName, String] = restoreFrom

  override def put(key: REName, value: String): Unit = values.put(key, value)
  override def get(key: REName): Option[String] = values.get(key)
  def snapshot(): mutable.Map[REName, String] = values

  override protected def makeTurn(priorTurn: Option[ReStoringTurn]): ReStoringTurn = new ReStoringTurn(this)
  override def schedulerName : String = s"InMemoryStorage"
  override def forceNewTransaction[R](initialWrites: Set[ReSource], admissionPhase: AdmissionTicket => R): R =
    synchronized(super.forceNewTransaction(initialWrites, admissionPhase))
}


