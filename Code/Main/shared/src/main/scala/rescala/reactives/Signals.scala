package rescala.reactives

import rescala.core._
import rescala.macros.cutOutOfUserComputation
import rescala.reactives.RExceptions.EmptySignalControlThrowable
import rescala.reactives.Signals.Sstate

import scala.concurrent.{ExecutionContext, Future}

/** Functions to construct signals, you probably want to use signal expressions in [[rescala.interface.RescalaInterface.Signal]] for a nicer API. */
object Signals {
  type Sstate[T, S <: Struct] = S#State[Pulse[T], S]

  private def ignore2[Tick, Current, Res](f: Tick => Res): (Tick, Current) => Res = (ticket, _) => f(ticket)


  /** creates a new static signal depending on the dependencies, reevaluating the function */
  @cutOutOfUserComputation
  def static[T, S <: Struct](dependencies: ReSource[S]*)
                            (expr: StaticTicket[S] => T)
                            (implicit ct: CreationTicket[S])
  : Signal[T, S] = {
    ct.create[Pulse[T], StaticSignal[T, S]](dependencies.toSet, Initializer.DerivedSignal, inite = true) {
      state => new StaticSignal[T, S](state, ignore2(expr), ct.rename)
    }
  }

  /** creates a signal that has dynamic dependencies (which are detected at runtime with Signal.apply(turn)) */
  @cutOutOfUserComputation
  def dynamic[T, S <: Struct](dependencies: ReSource[S]*)
                             (expr: DynamicTicket[S] => T)
                             (implicit ct: CreationTicket[S])
  : Signal[T, S] = {
    val staticDeps = dependencies.toSet
    ct.create[Pulse[T], DynamicSignal[T, S]](staticDeps, Initializer.DerivedSignal, inite = true) {
      state => new DynamicSignal[T, S](state, ignore2(expr), ct.rename, staticDeps)
    }
  }

  /** converts a future to a signal */
  @cutOutOfUserComputation
  def fromFuture[A: ReSerializable, S <: Struct](fut: Future[A])(implicit fac: Scheduler[S], ec: ExecutionContext): Signal[A, S] = {
    val v: Var[A, S] = rescala.reactives.Var.empty[A, S]
    fut.onComplete { res => fac.forceNewTransaction(v)(t => v.admitPulse(Pulse.tryCatch(Pulse.Value(res.get)))(t)) }
    v
  }

  @cutOutOfUserComputation
  def lift[A, S <: Struct, R](los: Seq[Signal[A, S]])(fun: Seq[A] => R)(implicit maybe: CreationTicket[S]): Signal[R, S] = {
    static(los: _*) { t => fun(los.map(s => t.dependStatic(s))) }
  }

  @cutOutOfUserComputation
  def lift[A1, B, S <: Struct](n1: Signal[A1, S])(fun: A1 => B)(implicit maybe: CreationTicket[S]): Signal[B, S] = {
    static(n1)(t => fun(t.dependStatic(n1)))
  }

  @cutOutOfUserComputation
  def lift[A1, A2, B, S <: Struct](n1: Signal[A1, S], n2: Signal[A2, S])(fun: (A1, A2) => B)(implicit maybe: CreationTicket[S]): Signal[B, S] = {
    static(n1, n2)(t => fun(t.dependStatic(n1), t.dependStatic(n2)))
  }

  class Diff[+A](val from: Pulse[A], val to: Pulse[A]) {

    def _1: A = from.get
    def _2: A = to.get
    def pair: (A, A) = {
      try {
        val right = to.get
        val left = from.get
        left -> right
      } catch {
        case EmptySignalControlThrowable => throw new NoSuchElementException(s"Can not convert $this to pair")
      }
    }

    override def toString: String = s"Diff($from, $to)"
  }

  object Diff {
    def apply[A](from: Pulse[A], to: Pulse[A]): Diff[A] = new Diff(from, to)
    def unapply[A](arg: Diff[A]): Option[(A, A)] = arg.from match {
      case Pulse.Value(v1) => arg.to match {
        case Pulse.Value(v2) => Some((v1, v2))
        case _ => None
      }
      case _ => None
    }
  }

  private[rescala] def computeNewValue[T, S <: Struct](rein: ReevTicket[Pulse[T], S], newValue: () => T): ReevTicket[Pulse[T], S] = {
    val newPulse = Pulse.tryCatch(Pulse.diffPulse(newValue(), rein.before))
    if (newPulse.isChange) rein.withValue(newPulse) else rein
  }

}

sealed private abstract class SignalImpl[T, S <: Struct](initial: Sstate[T, S],
                                                         expr: (DynamicTicket[S], () => T) => T,
                                                         name: REName,
                                                         staticDeps: Set[ReSource[S]])
  extends Base[Pulse[T], S](initial, name) with Derived[S] with Signal[T, S] {

  override protected[rescala] def reevaluate(rein: ReIn): Rout = {
    rein.trackDependencies(staticDeps)
    Signals.computeNewValue[T, S](rein, () => expr(rein, () => rein.before.get))
  }
}


final private class StaticSignal[T, S <: Struct](initial: Sstate[T, S],
                                                 expr: (StaticTicket[S], () => T) => T,
                                                 name: REName)
  extends SignalImpl[T, S](initial, expr, name, null) with DisconnectableImpl[S]

final private class DynamicSignal[T, S <: Struct](initial: Sstate[T, S],
                                                  expr   : (DynamicTicket[S], () => T) => T,
                                                  name   : REName,
                                                  staticDeps: Set[ReSource[S]])
  extends SignalImpl[T, S](initial, expr, name, staticDeps) with DisconnectableImpl[S]


