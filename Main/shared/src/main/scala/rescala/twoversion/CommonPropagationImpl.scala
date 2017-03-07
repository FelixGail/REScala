package rescala.twoversion

import rescala.graph.{GraphStruct, DepDiff, Pulse, Reactive}

import scala.util.control.NonFatal

/**
  * Basic implementation of the most fundamental propagation steps as defined by AbstractPropagation.
  * Only compatible with spore definitions that store a pulse value and support graph operations.
  *
  * @tparam S Struct type that defines the spore type used to manage the reactive evaluation
  */
trait CommonPropagationImpl[S <: GraphStruct] extends AbstractPropagation[S] {
  private val toCommit = new java.util.ArrayList[Committable[S]]()
  private val observers = new java.util.ArrayList[() => Unit]()

  override def schedule(commitable: Committable[S]): Unit = toCommit.add(commitable)

  override def observe(f: () => Unit): Unit = observers.add(f)

  def setIfChange(r: Reactive[S])(value: Pulse[r.Value])(implicit ticket: S#Ticket[S]): Boolean = {
    val differs = value != r.state.base
    val changed = differs && value.isChange
    if (changed) r.state.set(value)
    else if (differs) r.state.set(r.state.base)
    changed
  }

  override def commitPhase(): Unit = {
    val it = toCommit.iterator()
    while (it.hasNext) it.next().commit(this)
  }

  override def rollbackPhase(): Unit = {
    val it = toCommit.iterator()
    while (it.hasNext) it.next().release(this)
  }

  override def observerPhase(): Unit = {
    val it = observers.iterator()
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

  protected def discover(sink: Reactive[S])(source: Reactive[S])(implicit ticket: S#Ticket[S]): Unit = source.state.discover(sink)

  protected def drop(sink: Reactive[S])(source: Reactive[S])(implicit ticket: S#Ticket[S]): Unit = source.state.drop(sink)

  final def applyDiff(head: Reactive[S], diff: DepDiff[S])(implicit ticket: S#Ticket[S]): Unit = {
    head.state.updateIncoming(diff.novel)
    diff.removed foreach drop(head)
    diff.added foreach discover(head)
  }


}
