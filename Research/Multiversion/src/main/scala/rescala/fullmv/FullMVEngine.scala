package rescala.fullmv

import java.util.concurrent.locks.ReentrantLock

import rescala.core.SchedulerImpl
import rescala.fullmv.tasks.{Framing, SourceNotification}

import scala.util.Try

class FullMVEngine(val name: String) extends SchedulerImpl[FullMVStruct, FullMVTurn] {
  val lock: ReentrantLock = new ReentrantLock()

  val dummy: FullMVTurn = {
    val dummy = new FullMVTurn(this, null)
    dummy.beginExecuting()
    dummy.completeExecuting()
    if(FullMVEngine.DEBUG) println(s"[${Thread.currentThread().getName}] $this SETUP COMPLETE")
    dummy
  }
  def newTurn(): FullMVTurn = new FullMVTurn(this, Thread.currentThread())

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

      // propagation phase
      if (setWrites.nonEmpty) {
        turn.initialChanges = admissionTicket.initialChanges.map(ic => ic.source -> ic).toMap
        for(write <- setWrites) turn.pushLocalTask(SourceNotification(turn, write, admissionResult.isSuccess && turn.initialChanges.contains(write)))
      }

      // turn completion
      turn.completeExecuting()

      // wrap-up "phase"
      val transactionResult = if(admissionTicket.wrapUp == null){
        admissionResult
      } else {
        val wrapUpTicket = new WrapUpTicket(){
          override def access(reactive: ReSource): reactive.Value = turn.dynamicAfter(reactive)
        }
        admissionResult.map { i =>
          // executed in map call so that exceptions in wrapUp make the transaction result a Failure
          admissionTicket.wrapUp(wrapUpTicket)
          i
        }
      }

      // result
      transactionResult.get
    }
  }

  override def toString: String = "Turns " + name
}

object FullMVEngine {
  val DEBUG = false

  val default = new FullMVEngine("default")
}
