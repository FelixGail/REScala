package rescala.fullmv

import rescala.core.SchedulerImpl
import rescala.fullmv.tasks.{Framing, SourceNotification}

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.Try

class FullMVEngine(val timeout: Duration, val name: String) extends SchedulerImpl[FullMVStruct, FullMVTurn] {
  override val dummy: FullMVTurnImpl = {
    val dummy = new FullMVTurnImpl(this, Host.dummyGuid, null, lockHost.newLock())
    dummy.beginExecuting()
    dummy.completeExecuting()
    if(Host.DEBUG || SubsumableLock.DEBUG || FullMVEngine.DEBUG) println(s"[${Thread.currentThread().getName}] $this SETUP COMPLETE")
    dummy
  }
  def newTurn(): FullMVTurnImpl = createLocal(new FullMVTurnImpl(this, _, Thread.currentThread(), lockHost.newLock()))

  override private[rescala] def singleReadValueOnce[A](reactive: Signal[A]) = reactive.state.latestValue.get

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
      val admissionTicket = new AdmissionTicket(turn) {
        override def access[A](reactive: Signal[A]): reactive.Value = turn.dynamicBefore(reactive)
      }
      val admissionResult = Try { admissionPhase(admissionTicket) }
      if (FullMVEngine.DEBUG) admissionResult match {
        case scala.util.Failure(e) => e.printStackTrace()
        case _ =>
      }
      assert(turn.activeBranches.get == 0, s"Admission phase left ${turn.activeBranches.get()} tasks undone.")

      // propagation phase
      if (setWrites.nonEmpty) {
        turn.initialChanges = admissionTicket.initialChanges.map(ic => ic.source -> ic).toMap
        turn.activeBranchDifferential(TurnPhase.Executing, setWrites.size)
        for(write <- setWrites) threadPool.submit(SourceNotification(turn, write, admissionResult.isSuccess && turn.initialChanges.contains(write)))
      }

      // wrap-up "phase" (executes in parallel with propagation)
      val transactionResult = if(admissionTicket.wrapUp == null){
        admissionResult
      } else {
        val wrapUpTicket = new WrapUpTicket(){
          override def access(reactive: ReSource): reactive.Value = turn.dynamicAfter(reactive)
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

  override def toString: String = "Turns " + name
  def cacheStatus: String = s"${instances.size()} turn instances and ${lockHost.instances.size()} lock instances"
}

object FullMVEngine {
  val DEBUG = false

  val default = new FullMVEngine(10.seconds, "default")
}
