package rescala.reactives

import rescala.core._
import rescala.reactives.RExceptions.UnhandledFailureException

/**
  * Generic interface for observers that represent a function registered to trigger for every reevaluation of a reactive value.
  * Currently this interface is only used to allow a removal of registered observer functions.
  *
  * @tparam S Struct type used for the propagation of the signal
  */
trait Observe[S <: Struct] {
  def remove()(implicit fac: Engine[S]): Unit
}

object Observe {

  private val strongObserveReferences = scala.collection.mutable.HashSet[Observe[_]]()

  private abstract class Obs[T, S <: Struct](bud: S#State[Pulse[Unit], S], dependency: Pulsing[Pulse[T], S], fun: T => Unit, fail: Throwable => Unit) extends Base[Unit, S](bud) with Reactive[S] with Observe[S]  {
    this: Disconnectable[S] =>

    override protected[rescala] def reevaluate(ticket: Turn[S], before: Pulse[Unit], indeps: Set[Reactive[S]]): ReevaluationResult[Value, S] = {
      scheduleHandler(this, ticket, dependency, fun, fail)
      ReevaluationResult.Static(Pulse.NoChange)
    }
    override def remove()(implicit fac: Engine[S]): Unit = {
      disconnect()
      strongObserveReferences.synchronized(strongObserveReferences.remove(this))
    }
  }

  private def scheduleHandler[T, S <: Struct](obs: Obs[T,S], turn:Turn[S], dependency: Pulsing[Pulse[T], S], fun: T => Unit, fail: Throwable => Unit) = {
    turn.makeStaticReevaluationTicket().staticDepend(dependency) match {
      case Pulse.NoChange =>
      case Pulse.empty =>
      case Pulse.Value(v) => turn.observe(() => fun(v))
      case Pulse.Exceptional(t) =>
        if (fail eq null) throw new UnhandledFailureException(obs, t)
        else turn.observe(() => fail(t))
    }
  }

  def weak[T, S <: Struct](dependency: Pulsing[Pulse[T], S])(fun: T => Unit, fail: Throwable => Unit)(implicit maybe: CreationTicket[S]): Observe[S] = {
    maybe(initTurn => initTurn.create[Pulse[Unit], Obs[T, S]](Set(dependency), ValuePersistency.Event) { state =>
      new Obs[T, S](state, dependency, fun, fail) with Disconnectable[S]
    })
  }

  def strong[T, S <: Struct](dependency: Pulsing[Pulse[T], S])(fun: T => Unit, fail: Throwable => Unit)(implicit maybe: CreationTicket[S]): Observe[S] = {
    val obs = weak(dependency)(fun, fail)
    strongObserveReferences.synchronized(strongObserveReferences.add(obs))
    obs
  }

}

/**
  * Reactives that can be observed by a function outside the reactive graph
  *
  * @tparam P Value type stored by the pulse of the reactive value
  * @tparam S Struct type that defines the spore type used to manage the reactive evaluation
  */
trait Observable[+P, S <: Struct] {
  this : Pulsing[Pulse[P], S] =>
  /** add an observer */
  final def observe(
    onSuccess: P => Unit,
    onFailure: Throwable => Unit = null
  )(implicit ticket: CreationTicket[S]): Observe[S] = Observe.strong(this)(onSuccess, onFailure)
}
