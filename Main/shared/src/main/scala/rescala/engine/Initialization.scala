package rescala.engine

import rescala.graph._


trait InitializationImpl[S <: Struct] extends Turn[S] {
  final private[rescala] def create[P, T <: Reactive[S]](incoming: Set[Reactive[S]], dynamic: Boolean, valuePersistency: ValuePersistency[P])(instantiateReactive: S#State[P, S] => T): T = {
    val state = makeStructState(valuePersistency)
    val reactive = instantiateReactive(state)
    ignite(reactive, incoming, dynamic, valuePersistency)
    reactive
  }

  /**
    * to be implemented by the scheduler, called when a new state storage object is required for instantiating a new reactive.
    * @param valuePersistency the value persistency
    * @tparam P the stored value type
    * @return the initialized state storage
    */
  protected def makeStructState[P](valuePersistency: ValuePersistency[P]): S#State[P, S]

  /**
    * to be implemented by the propagation algorithm, called when a new reactive has been instantiated and needs to be connected to the graph and potentially reevaluated.
    * @param reactive the newly instantiated reactive
    * @param incoming a set of incoming dependencies
    * @param dynamic false if the set of incoming dependencies is the correct final set of dependencies (static reactive)
    *                true if the set of incoming dependencies is just a best guess for the initial dependencies.
    */
  protected def ignite(reactive: Reactive[S], incoming: Set[Reactive[S]], dynamic: Boolean, valuePersistency: ValuePersistency[_]): Unit
}



sealed trait ValuePersistency[+V] {
  val initialValue: V
  val isTransient: Boolean
  val ignitionRequiresReevaluation: Boolean
}
case object Transient extends ValuePersistency[Pulse[Nothing]] {
  override val initialValue: Pulse[Nothing] = Pulse.NoChange
  override val isTransient: Boolean = true
  override val ignitionRequiresReevaluation: Boolean = false
}
sealed trait Steady[+V] extends ValuePersistency[V] {
  override val isTransient: Boolean = false
}
case object Derived extends Steady[Change[Nothing]] {
  override val initialValue: Change[Nothing] = Pulse.empty
  override val ignitionRequiresReevaluation: Boolean = true
}
case class Accumulating[V](override val initialValue: Change[V]) extends Steady[Change[V]] {
  override val ignitionRequiresReevaluation: Boolean = false
}
