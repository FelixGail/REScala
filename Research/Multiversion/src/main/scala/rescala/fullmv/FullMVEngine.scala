package rescala.fullmv

import java.util.concurrent.Executor

import rescala.core.{EngineImpl, ReSourciV}
import rescala.fullmv.tasks._

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}
import scala.concurrent.duration._
import scala.util.Try

class FullMVEngine(val timeout: Duration, val name: String) extends EngineImpl[FullMVStruct, FullMVTurn] {
  def newTurn(): FullMVTurn = new FullMVTurn(this, Thread.currentThread())
  val dummy: FullMVTurn = {
    val dummy = new FullMVTurn(this, null)
    dummy.phase = TurnPhase.Completed
    dummy
  }

  override private[rescala] def singleNow[A](reactive: ReSourciV[A, FullMVStruct]) = reactive.state.latestValue

  override private[rescala] def executeTurn[R](declaredWrites: Traversable[ReSource], admissionPhase: (AdmissionTicket) => R): R = {
    val turn = newTurn()
    withTurn(turn) {
      val setWrites = declaredWrites.toSet // this *should* be part of the interface..
      if (setWrites.nonEmpty) {
        // framing phase
        turn.beginFraming()
        for (i <- setWrites) turn.pushLocalTask(Framing(turn, i))
        turn.completeFraming()
      } else {
        turn.beginExecuting()
      }

      // admission phase
      val admissionTicket = turn.makeAdmissionPhaseTicket()
      val admissionResult = Try { admissionPhase(admissionTicket) }
      if (FullMVEngine.DEBUG) admissionResult match {
        case scala.util.Failure(e) => e.printStackTrace()
        case _ =>
      }
      assert(turn.localTaskQueue.isEmpty, s"Admission phase left ${turn.localTaskQueue.size()} tasks undone.")
      assert(turn.externallyPushedTasks.get == Nil, s"Admission phase left ${turn.externallyPushedTasks.get.size} external tasks undone.")

      // propagation phase
      if (setWrites.nonEmpty) {
        turn.initialChanges = admissionTicket.initialChanges
        for(write <- setWrites) {
          turn.pushLocalTask(SourceNotification(turn, write, admissionResult.isSuccess && admissionTicket.initialChanges.contains(write)))
        }
      }

      // wrap-up "phase" (executes in parallel with propagation)
      val transactionResult = if(admissionTicket.wrapUp == null){
        admissionResult
      } else {
        val wrapUpTicket = turn.makeWrapUpPhaseTicket()
        admissionResult.map{ i =>
          // executed in map call so that exceptions in wrapUp make the transaction result a Failure
          admissionTicket.wrapUp(wrapUpTicket)
          i
        }
      }

      // turn completion
      turn.completeExecuting()

      // result
      transactionResult.get
    }
  }

  override def toString: String = "Host " + name
}

object FullMVEngine {
  val DEBUG = false

  val default = new FullMVEngine(10.seconds, "default")

  val notWorthToMoveToTaskpool: ExecutionContextExecutor = ExecutionContext.fromExecutor(new Executor{
    override def execute(command: Runnable): Unit = command.run()
  })
}
