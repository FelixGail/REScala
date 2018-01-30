package rescala.reactives

import rescala.core._
import rescala.reactives.Events.Estate

abstract class Source[T, S <: Struct, N](initialState: S#State[Pulse[T], S, N], name: REName) extends RENamed(name) with ReSourciV[Pulse[T], S] {
  override type Value = Pulse[T]
  override type Notification = N
  final override protected[rescala] def state: S#State[Pulse[T], S, N] = initialState

  final def admit(value: T)(implicit ticket: AdmissionTicket[S]): Unit = admitPulse(Pulse.Value(value))
  final def admitPulse(pulse: Pulse[T])(implicit ticket: AdmissionTicket[S]): Unit = {
    ticket.recordChange(new InitialChange[S] {
      override val source: Source.this.type = Source.this
      override def value: Value = pulse
      override def accept(before: Pulse[T]): Boolean = before != value
    })
  }
}

/** Source events with imperative occurrences
  *
  * @param initialState of by the event
  * @tparam T Type returned when the event fires
  * @tparam S Struct type used for the propagation of the event
  */
final class Evt[T, S <: Struct] private[rescala](initialState: Estate[S, T], name: REName) extends Source[T, S, Pulse[T]](initialState, name) with Event[T, S] {
  /** Trigger the event */
  @deprecated("use .fire instead of apply", "0.21.0")
  def apply(value: T)(implicit fac: Scheduler[S]): Unit = fire(value)
  def fire()(implicit fac: Scheduler[S], ev: Unit =:= T): Unit = fire(ev(Unit))(fac)
  def fire(value: T)(implicit fac: Scheduler[S]): Unit = fac.transaction(this) {admit(value)(_)}
  override def disconnect()(implicit engine: Scheduler[S]): Unit = ()
}

/** Creates new [[Evt]]s */
object Evt {
  def apply[T, S <: Struct]()(implicit ticket: CreationTicket[S]): Evt[T, S] = ticket { t =>
    t.createSource[Pulse[T], Evt[T, S], Pulse[T]](Initializer.Event)(new Evt[T, S](_, ticket.rename))
  }
}

/** Source signals with imperatively updates.
  *
  * @param initialState of the signal
  * @tparam A Type stored by the signal
  * @tparam S Struct type used for the propagation of the signal
  */
final class Var[A, S <: Struct] private[rescala](initialState: Signals.Sstate[S, A], name: REName) extends Source[A, S, Nothing](initialState, name) with Signal[A, S] {
  //def update(value: A)(implicit fac: Engine[S]): Unit = set(value)
  def set(value: A)(implicit fac: Scheduler[S]): Unit = fac.transaction(this) {admit(value)(_)}

  def transform(f: A => A)(implicit fac: Scheduler[S]): Unit = fac.transaction(this) { t =>
    admit(f(t.now(this)))(t)
  }

  def setEmpty()(implicit fac: Scheduler[S]): Unit = fac.transaction(this)(t => admitPulse(Pulse.empty)(t))

  override def disconnect()(implicit engine: Scheduler[S]): Unit = ()
}

/** Creates new [[Var]]s */
object Var {
  def apply[T: ReSerializable, S <: Struct](initval: T)(implicit ticket: CreationTicket[S]): Var[T, S] = fromChange(Pulse.Value(initval))
  def empty[T: ReSerializable, S <: Struct]()(implicit ticket: CreationTicket[S]): Var[T, S] = fromChange(Pulse.empty)
  private[this] def fromChange[T: ReSerializable, S <: Struct](change: Pulse.Change[T])(implicit ticket: CreationTicket[S]): Var[T, S] = ticket { t =>
    t.createSource[Pulse[T], Var[T, S], Nothing](Initializer.InitializedSignal(change))(new Var[T, S](_, ticket.rename))
  }
}

