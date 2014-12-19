package rescala.synchronization

import rescala.graph.Reactive
import rescala.turns.Turn

trait InterturnDependencyChanges extends Turn {
  self: Prelock =>
  /** registering a dependency on a node we do not personally own does require some additional care.
    * we move responsibility to the commit phase */
  abstract override def register(sink: Reactive)(source: Reactive): Unit = {
    source.lock.acquireDynamic(key)
    val owner = source.lock.getOwner
    if ((owner ne key) && !source.dependants.get.contains(sink)) {
      owner.turn.register(sink)(source)
      owner.turn.admit(sink)
      SyncUtil.wantReachable(owner, sink)
    }
    else {
      sink.lock.wantThis.putAll(source.lock.wantThis)
      super.register(sink)(source)
    }
  }

  /** this is for cases where we register and then unregister the same dependency in a single turn */
  abstract override def unregister(sink: Reactive)(source: Reactive): Unit = {
    source.lock.acquireDynamic(key)
    val owner = source.lock.getOwner
    if (owner ne key) {
      owner.turn.forget(sink)
      owner.turn.unregister(sink)(source)
    }
    else super.unregister(sink)(source)
  }
}