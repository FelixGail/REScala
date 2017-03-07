package rescala.graph

import rescala.engine.Engine
import rescala.propagation.{ReevaluationTicket, Turn}

/**
  * Indicator for the result of a re-evaluation of a reactive value.
  */
sealed trait ReevaluationResult[A, S <: Struct]

object ReevaluationResult {

  /**
    * Result of the static re-evaluation of a reactive value.
    */
  case class Static[A, S <: Struct](value: Pulse[A]) extends ReevaluationResult[A, S]

  /**
    * Result of the dynamic re-evaluation of a reactive value.
    * When using a dynamic dependency model, the dependencies of a value may change at runtime if it is re-evaluated
    */
  case class Dynamic[A, S <: Struct](value: Pulse[A], dependencies: Set[Reactive[S]]) extends ReevaluationResult[A, S]
}

/**
  * Calculates and stores added or removed dependencies of a reactive value.
  *
  * @param novel Set of dependencies after re-evaluation
  * @param old   Set of dependencies before re-evaluation
  * @tparam S Struct type that defines the spore type used to manage the reactive evaluation
  */
case class DepDiff[S <: Struct](novel: Set[Reactive[S]], old: Set[Reactive[S]]) {
  lazy val added: Set[Reactive[S]] = novel.diff(old)
  lazy val removed: Set[Reactive[S]] = old.diff(novel)
}

/**
  * Implementation of static re-evaluation of a reactive value.
  * Only calculates the stored value of the pulse and compares it with the old value.
  *
  * @tparam S Struct type that defines the spore type used to manage the reactive evaluation
  */
trait StaticReevaluation[S <: Struct] extends Reactive[S] {
  this: Pulsing[_, S] =>

  /** side effect free calculation of the new pulse for the current turn */
  protected[rescala] def calculatePulse()(implicit turn: Turn[S]): Pulse[Value]

  override protected[rescala] def reevaluate()(implicit turn: Turn[S]): ReevaluationResult[Value, S] =  {
    ReevaluationResult.Static(calculatePulse())
  }


}


/**
  * Implementation of dynamic re-evaluation of a reactive value.
  * Calculates the pulse and new dependencies, compares them with the old value and dependencies and returns the result.
  *
  * @tparam S Struct type that defines the spore type used to manage the reactive evaluation
  */
trait DynamicReevaluation[S <: Struct] extends Reactive[S] {
  this: Pulsing[_, S] =>


  /** side effect free calculation of the new pulse and the new dependencies for the current turn */
  def calculatePulseDependencies(turn: ReevaluationTicket[S]): Pulse[Value]

  override protected[rescala] def reevaluate()(implicit turn: Turn[S]): ReevaluationResult[Value, S] = {
    val ticket = new ReevaluationTicket(turn)
    val newPulse = calculatePulseDependencies(ticket)
    ReevaluationResult.Dynamic(newPulse, ticket.collectedDependencies)
  }
}

trait Disconnectable[S <: Struct] extends Reactive[S] {

  @volatile private var disconnected = false

  final def disconnect()(implicit engine: Engine[S, Turn[S]]): Unit = {
    engine.plan(this) { turn =>
      disconnected = true
    }
  }


  abstract final override protected[rescala] def reevaluate()(implicit turn: Turn[S]): ReevaluationResult[Value, S] = {
    if (disconnected) {
      ReevaluationResult.Dynamic(Pulse.NoChange, Set.empty)
    }
    else {
      super.reevaluate()
    }
  }

}
