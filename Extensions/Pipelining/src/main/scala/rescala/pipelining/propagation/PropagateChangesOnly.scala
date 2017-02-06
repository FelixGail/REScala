package rescala.pipelining.propagation

import rescala.graph.ReevaluationResult.{Dynamic, Static}
import rescala.graph.{Reactive, ReevaluationResult}
import rescala.pipelining.{PipelineStruct, PipeliningTurn}

private[pipelining] trait PropagateChangesOnly {

  self : PipeliningTurn =>

  type S = PipelineStruct.type

   override protected def calculateQueueAction(head : Reactive[S], result : ReevaluationResult[S]) : (Boolean, Int, QueueAction) =
     result match {
      case Static(true) =>
        (true, -1, EnqueueDependencies)
      case Static(false) =>
        (false, -1, DoNothing)
      case Dynamic(hasChanged, diff) =>
        head.state.updateIncoming(diff.novel)
        diff.removed foreach drop(head)
        diff.added foreach discover(head)
        val newLevel = maximumLevel(diff.novel) + 1
        if (head.state.level < newLevel)
          (hasChanged, newLevel, RequeueReactive)
        else if (hasChanged)
          (true, newLevel, EnqueueDependencies)
        else (false, newLevel, DoNothing)
    }

   def propagationPhase(): Unit = levelQueue.evaluateQueue(evaluate)


}
