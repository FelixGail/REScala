package rescala.twoversion

import rescala.core._

import scala.util.control.NonFatal

/**
  * Basic implementation of the most fundamental propagation steps as defined by AbstractPropagation.
  * Only compatible with spore definitions that store a pulse value and support graph operations.
  *
  * @tparam S Struct type that defines the spore type used to manage the reactive evaluation
  */
trait TwoVersionPropagationImpl[S <: TwoVersionStruct] extends TwoVersionPropagation[S] with Creation[S] {
  outer =>

  private var _token: Token = Token()
  def token: Token = _token

  private val toCommit = scala.collection.mutable.ArrayBuffer[Committable[S]]()
  private val observers = scala.collection.mutable.ArrayBuffer[() => Unit]()

  def clear(): Unit = {
    toCommit.clear()
    observers.clear()
    _token = Token()
  }

  override def schedule(commitable: Committable[S]): Unit = toCommit += commitable

  def observe(f: () => Unit): Unit = observers += f

  override def commitPhase(): Unit = {
    val it = toCommit.iterator
    while (it.hasNext) it.next().commit(this)
  }

  override def rollbackPhase(): Unit = {
    val it = toCommit.iterator
    while (it.hasNext) it.next().release(this)
  }

  override def observerPhase(): Unit = {
    val it = observers.iterator
    var failure: Throwable = null
    while (it.hasNext) {
      try {
        it.next().apply()
      }
      catch {
        case NonFatal(e) => failure = e
      }
    }
    // find the first failure and rethrow the contained exception
    // we should probably aggregate all of the exceptions,
    // but this is not the place to invent exception aggregation
    if (failure != null) throw failure
  }


  def initialize(ic: InitialChange[S]): Unit

  final override def initializationPhase(initialChanges: Traversable[InitialChange[S]]): Unit = initialChanges.foreach { ic =>
    if (ic.accept(ic.source.state.base(token))) {
      initialize(ic)
    }
  }

  final def commitDependencyDiff(node: Reactive[S], current: Set[ReSource[S]])(updated: Set[ReSource[S]]): Unit = {
    val indepsRemoved = current -- updated
    val indepsAdded = updated -- current
    indepsRemoved.foreach(drop(_, node))
    indepsAdded.foreach(discover(_, node))
    writeIndeps(node, updated)
  }

  private[rescala] def discover(node: ReSource[S], addOutgoing: Reactive[S]): Unit = node.state.discover(addOutgoing)
  private[rescala] def drop(node: ReSource[S], removeOutgoing: Reactive[S]): Unit = node.state.drop(removeOutgoing)

  private[rescala] def writeIndeps(node: Reactive[S], indepsAfter: Set[ReSource[S]]): Unit = node.state.updateIncoming(indepsAfter)

  /** allow turn to handle dynamic access to reactives */
  def dynamicDependencyInteraction(dependency: ReSource[S]): Unit


  override private[rescala] def makeAdmissionPhaseTicket() = new AdmissionTicket[S](this) {
    override def read[A](reactive: ReSourciV[A, S]): A = {
      dynamicDependencyInteraction(reactive)
      reactive.state.base(token)
    }
  }
  private[rescala] def makeDynamicReevaluationTicket[V](): ReevTicket[V, S] = new ReevTicket[V, S](this) {
    override def dynamicAfter[A](reactive: ReSourciV[A, S]): A = TwoVersionPropagationImpl.this.dynamicAfter(reactive)
    override def staticAfter[A](reactive: ReSourciV[A, S]): A = reactive.state.get(token)
  }

  private[rescala] def makeWrapUpPhaseTicket(): WrapUpTicket[S] = new WrapUpTicket[S] {
    override def dynamicAfter[A](reactive: ReSourciV[A, S]): A = TwoVersionPropagationImpl.this.dynamicAfter(reactive)
  }

  private[rescala] def dynamicAfter[P](reactive: ReSourciV[P, S]) = {
    // Note: This only synchronizes reactive to be serializable-synchronized, but not glitch-free synchronized.
    // Dynamic reads thus may return glitched values, which the reevaluation handling implemented in subclasses
    // must account for by repeating glitched reevaluations!
    dynamicDependencyInteraction(reactive)
    reactive.state.get(token)
  }
  def writeState(pulsing: ReSource[S])(value: pulsing.Value): Unit = {
    if (pulsing.state.write(value, token)) this.schedule(pulsing.state)
  }
}
