package rescala.fullmv.api

import java.util.concurrent.atomic.AtomicInteger

sealed trait Phase
object Preparing extends Phase
object Running extends Phase
object Completed extends Phase
object Obsolete extends Phase

trait Transaction {
  def phase: Phase
  def branches(delta: Int): Unit
  def start(): this.type
  def done(): this.type
}

object Transaction {
  def apply(): Transaction = new CounterIdTransactionImpl
}

object CounterIdTransactionImpl {
  val counter = new AtomicInteger(0)
}
class CounterIdTransactionImpl extends Transaction {
  val id = CounterIdTransactionImpl.counter.getAndIncrement()
  var phase: Phase = Preparing
  var branches = new AtomicInteger(0)

  override def branches(delta: Int) = branches.addAndGet(delta)
  override def start(): this.type = synchronized {
    if(branches.get() != 0) throw new IllegalStateException("Cannot switch phases due to "+branches+" still-active branches!")
    if(phase != Preparing) throw new IllegalStateException("Cannot start from phase "+phase)
    phase = Running
    notifyAll()
    this
  }
  override def done(): this.type = synchronized {
    if(branches.get() != 0) throw new IllegalStateException("Cannot switch phases due to "+branches+" still-active branches!")
    if(phase != Running) throw new IllegalStateException("Cannot complete from phase "+phase)
    phase = Completed
    notifyAll()
    this
  }
  override def toString: String = "Transaction("+id+","+phase+")"
}
