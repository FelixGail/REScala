package rescala.core

import rescala.RescalaInterface

import scala.annotation.implicitNotFound
import scala.util.DynamicVariable

/**
  * Propagation engine that defines the basic data-types available to the user and creates turns for propagation handling
  *
  * @tparam S Struct type that defines the spore type used to manage the reactive evaluation
  */
@implicitNotFound(msg = "Could not find an implicit propagation engine. Did you forget an import?")
trait Scheduler[S <: Struct] extends RescalaInterface[S] {

  override def explicitEngine: this.type = this

  private[rescala] def executeTurn[R](initialWrites: Traversable[ReSource], admissionPhase: AdmissionTicket => R): R
  private[rescala] def singleNow[A](reactive: ReSourciV[A, S]): A
  private[rescala] def create[T](f: (Creation) => T): T
}


trait SchedulerImpl[S <: Struct, ExactTurn <: Creation[S]] extends Scheduler[S] {

  override private[rescala] def create[T](f: (Creation) => T) = {
    _currentTurn.value match {
      case Some(turn) => f(turn)
      case None => executeTurn(Set.empty, ticket => f(ticket.cas))
    }
  }

  protected val _currentTurn: DynamicVariable[Option[ExactTurn]] = new DynamicVariable[Option[ExactTurn]](None)
  private[rescala] def withTurn[R](turn: ExactTurn)(thunk: => R): R = _currentTurn.withValue(Some(turn))(thunk)
}

