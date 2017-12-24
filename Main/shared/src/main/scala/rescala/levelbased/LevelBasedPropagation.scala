package rescala.levelbased

import rescala.core.{InitialChange, ReSource, Reactive, ReevaluationResult}
import rescala.twoversion.TwoVersionPropagationImpl

/**
  * Further implementation of level-based propagation based on the common propagation implementation.
  *
  * @tparam S Struct type that defines the spore type used to manage the reactive evaluation
  */
trait LevelBasedPropagation[S <: LevelStruct] extends TwoVersionPropagationImpl[S] with LevelQueue.Evaluator[S] {
  private var _changed = List.empty[ReSource[S]]

  val levelQueue = new LevelQueue[S](this)(this)

  override def clear(): Unit = {
    super.clear()
    _changed = Nil
  }

  override def evaluate(head: Reactive[S]): Unit = {
    val res = head.reevaluate(this, head.state.base(token), head.state.incoming(this))
    if (!res.indepsChanged) {
      applyResult(head)(res)
    } else {
      val newLevel = maximumLevel(res.indepsAfter) + 1
      val redo = head.state.level(this) < newLevel
      if (redo) {
        levelQueue.enqueue(newLevel)(head)
      } else {
        res.commitDependencyDiff(this, head)
        applyResult(head, newLevel)(res)
      }
    }
  }

  private def applyResult(head: ReSource[S], minLevel: Int = -42)(res: ReevaluationResult[head.Value, S]): Unit = {
    if (res.valueChanged) {
      writeState(head)(res.value)
      head.state.outgoing(this).foreach(levelQueue.enqueue(-42))
      _changed ::= head
    }
  }

  private def maximumLevel(dependencies: Set[ReSource[S]]): Int = dependencies.foldLeft(-1)((acc, r) => math.max(acc, r.state.level(this)))

  override protected def ignite(reactive: Reactive[S], incoming: Set[ReSource[S]], ignitionRequiresReevaluation: Boolean): Unit = {
    val level = if (incoming.isEmpty) 0 else incoming.map(_.state.level(this)).max + 1
    reactive.state.updateLevel(level)(this)

    incoming.foreach { dep =>
      dynamicDependencyInteraction(dep)
      discover(dep, reactive)
    }
    reactive.state.updateIncoming(incoming)(this)

    if (ignitionRequiresReevaluation || incoming.exists(_changed.contains)) {
      if (level <= levelQueue.currentLevel()) {
        evaluate(reactive)
      } else {
        levelQueue.enqueue(level)(reactive)
      }
    }
  }

  override def initializationPhase(initialChanges: Traversable[InitialChange[S]]): Unit = initialChanges.foreach { ic =>
    applyResult(ic.r)(ic.v(ic.r.state.base(token)))
  }

  def propagationPhase(): Unit = levelQueue.evaluateQueue()
}
