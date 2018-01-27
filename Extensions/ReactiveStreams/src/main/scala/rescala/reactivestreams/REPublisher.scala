package rescala.reactivestreams


import org.reactivestreams.{Publisher, Subscriber, Subscription}
import rescala.core.{Base, Pulse, REName, ReSourciV, ReevTicket, Result, Scheduler, Struct, ValuePersistency}

import scala.util.{Failure, Success}


object REPublisher {

  def apply[T, S <: Struct](dependency: ReSourciV[Pulse[T], S])(implicit fac: Scheduler[S]): REPublisher[T, S] =
    new REPublisher[T, S](dependency, fac)


  class REPublisher[T, S <: Struct](dependency: ReSourciV[Pulse[T], S], fac: Scheduler[S]) extends Publisher[T] {

    override def subscribe(s: Subscriber[_ >: T]): Unit = {
      val sub = REPublisher.subscription(dependency, s, fac)
      s.onSubscribe(sub)
    }

  }

  class SubscriptionReactive[T, S <: Struct](
    bud: S#State[Pulse[T], S],
    dependency: ReSourciV[Pulse[T], S],
    subscriber: Subscriber[_ >: T],
    fac: Scheduler[S],
    name: REName
  ) extends Base[Pulse[T], S](bud, name) with Subscription {

    var requested: Long = 0
    var cancelled = false

    override protected[rescala] def reevaluate(ticket: ReevTicket[Value, S], before: Pulse[T]): Result[Value, S] = {
      ticket.dependStatic(dependency).toOptionTry match {
        case None => ticket
        case Some(tryValue) =>
          synchronized {
            while (requested <= 0 && !cancelled) wait(100)
            if (cancelled) {
              ticket.trackDependencies()
              ticket
            }
            else {
              requested -= 1
              tryValue match {
                case Success(v) =>
                  subscriber.onNext(v)
                  ticket
                case Failure(t) =>
                  subscriber.onError(t)
                  cancelled = true
                  ticket.trackDependencies()
                  ticket
              }
            }
          }
      }
    }

    override def cancel(): Unit = {
      synchronized {
        cancelled = true
        notifyAll()
      }
    }

    override def request(n: Long): Unit = synchronized {
      requested += n
      notifyAll()
    }
  }

  def subscription[T, S <: Struct](
    dependency: ReSourciV[Pulse[T], S],
    subscriber: Subscriber[_ >: T],
    fac: Scheduler[S]
  ): SubscriptionReactive[T, S] = {
    fac.transaction() { ticket =>
      ticket.creation.create[Pulse[T], SubscriptionReactive[T, S]](Set(dependency), ValuePersistency.DerivedSignal) { state =>
        new SubscriptionReactive[T, S](state, dependency, subscriber, fac, s"forSubscriber($subscriber)")
      }
    }
  }

}
