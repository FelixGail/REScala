package rescala.levelbased

import rescala.core.{InitialChange, ReSource, Reactive, ReevaluationResultWithValue, ReevaluationResultWithoutValue}
import rescala.twoversion.TwoVersionPropagationImpl

import scala.collection.mutable.ArrayBuffer

/**
  * Further implementation of level-based propagation based on the common propagation implementation.
  *
  * @tparam S Struct type that defines the spore type used to manage the reactive evaluation
  */
trait LevelBasedPropagation[S <: LevelStruct] extends TwoVersionPropagationImpl[S] with LevelQueue.Evaluator[S] {
  private val _propagating = ArrayBuffer[ReSource[S]]()

  val levelQueue = new LevelQueue[S](this)(this)

  override def clear(): Unit = {
    super.clear()
    _propagating.clear()
  }

  override def evaluate(head: Reactive[S]): Unit = {
    head.reevaluate(this, head.state.base(token), head.state.incoming(this)) match {
      case res @ ReevaluationResultWithValue(_, indepsChanged, indepsAfter, _, _, _, propagate) =>
        if (!indepsChanged) {
          applyResult(head)(res)
        } else {
          val newLevel = maximumLevel(indepsAfter) + 1
          val redo = head.state.level(this) < newLevel
          if (redo) {
            levelQueue.enqueue(newLevel)(head)
          } else {
            res.commitDependencyDiff(this, head)
            applyResult(head, newLevel)(res)
          }
        }
      case ReevaluationResultWithoutValue(propagate) =>
        if (propagate) enqueueOutgoing(head)
    }

  }

  private def applyResult(head: ReSource[S], minLevel: Int = -42)(res: ReevaluationResultWithValue[head.Value, S]): Unit = {
    if (res.valueChanged) {writeValue(head, minLevel)(res.value)}
    if (res.propagate) enqueueOutgoing(head, minLevel)

  }

  private def writeValue(head: ReSource[S], minLevel: Int = -42)(value: head.Value): Unit = {
    writeState(head)(value)
  }

  private def enqueueOutgoing(head: ReSource[S], minLevel: Int = -42) = {
    head.state.outgoing(this).foreach(levelQueue.enqueue(minLevel))
    _propagating += head
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

    if (ignitionRequiresReevaluation || incoming.exists(_propagating.contains)) {
      if (level <= levelQueue.currentLevel()) {
        evaluate(reactive)
      } else {
        levelQueue.enqueue(level)(reactive)
      }
    }
  }

  override def initializationPhase(initialChanges: Traversable[InitialChange[S]]): Unit = initialChanges.foreach { ic =>
    writeValue(ic.source)(ic.value)
    enqueueOutgoing(ic.source)
  }

  def propagationPhase(): Unit = levelQueue.evaluateQueue()
}
