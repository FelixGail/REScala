package rescala.simpleprop

import rescala.core.{Creation, ReevTicket, ReSource, ReSourciV, Reactive, Scheduler, Struct, ValuePersistency}

trait SimpleStruct extends Struct {
  override type State[P, S <: Struct] = SimpleState[P]
}

class SimpleState[V](var value: V, transient: Option[V]) {
  var outgoing: Set[Reactive[SimpleStruct]] = Set.empty
  var discovered = false
  var dirty = false
  def reset(): Unit = {
    discovered = false
    dirty = false
    transient.foreach(value = _)
  }
}

object SimpleCreation extends Creation[SimpleStruct] {
  override protected[this] def makeDerivedStructState[P](valuePersistency: ValuePersistency[P]): SimpleState[P] =
    new SimpleState[P](valuePersistency.initialValue, if (valuePersistency.isTransient) Some(valuePersistency.initialValue) else None)
  override protected[this] def ignite(reactive: Reactive[SimpleStruct], incoming: Set[ReSource[SimpleStruct]], ignitionRequiresReevaluation: Boolean): Unit = {

    incoming.foreach { dep =>
      dep.state.outgoing += reactive
    }

    if (ignitionRequiresReevaluation) {
      Util.evaluate(reactive, incoming)
    }
  }

}


class SimpleScheduler extends Scheduler[SimpleStruct] {
  override private[rescala] def executeTurn[R](initialWrites: Traversable[ReSource], admissionPhase: AdmissionTicket => R) = {

    val admissionTicket = new AdmissionTicket(SimpleCreation) {
      override def read[A](reactive: ReSourciV[A, SimpleStruct]): A = reactive.state.value
    }
    val admissionResult = admissionPhase(admissionTicket)
    val initials = admissionTicket.initialChanges.valuesIterator.collect {
      case ic if ic.accept(ic.source.state.value) =>
        ic.source.state.value = ic.value
        ic.source
    }.toSeq
    def initialOutgoing = initials.iterator.flatMap(_.state.outgoing)
    initialOutgoing.foreach(_.state.dirty = true)
    val order = initialOutgoing.map(Util.toposort).toSeq.reverse.flatten
    order.foreach(r => if(r.state.dirty) Util.evaluate(r, Set.empty))
    initials.foreach(_.state.reset)
    order.foreach(_.state.reset)
    if (admissionTicket.wrapUp != null) ???
    admissionResult
  }
  override private[rescala] def singleNow[A](reactive: ReSourciV[A, SimpleStruct]) = reactive.state.value
  override private[rescala] def create[T](f: Creation => T) = f(SimpleCreation)
}


object Util {
  def toposort(rem: Reactive[SimpleStruct]): List[Reactive[SimpleStruct]] = {
    if (rem.state.discovered) Nil
    else {
      rem.state.discovered = true
      rem :: rem.state.outgoing.toList.flatMap(toposort)
    }
  }

  def evaluate(reactive: Reactive[SimpleStruct], incoming: Set[ReSource[SimpleStruct]]): Unit = {
    val dt = new ReevTicket[SimpleStruct](SimpleCreation) {
      override def dynamicAfter[A](reactive: ReSourciV[A, SimpleStruct]): A = ???
      override def staticAfter[A](reactive: ReSourciV[A, SimpleStruct]): A = reactive.state.value
    }
    val reev = reactive.reevaluate(dt, reactive.state.value)
    if (reev.propagate) reactive.state.outgoing.foreach(_.state.dirty = true)
    if (dt.getDependencies().isDefined) ???
    reev.forValue(reactive.state.value = _)
    reev.forEffect(_())
  }
}
