package scala.events
package behaviour
import scala.collection.mutable._

import scala.events.scalareact

/**
 * Wraps a scala.react reactive (including events) into an EScala event stream
 */
class EventStreamWrapper[P, V](reactive: scalareact.Reactive[P, V]) extends scalareact.Observing {
  protected val e = new ImperativeEvent[P]
  val event: Event[P] = e
  
  observe(reactive){
    value =>  e(value)
  }
}

/**
 * Wraps a scala.react reactive into an EScala reactive
 */
trait ReactivityWrapper[A, V] {  self: scalareact.TotalReactive[A, V] =>
   protected lazy val esWrapper = new EventStreamWrapper(self)
   
   lazy val changed: Event[A] = esWrapper.event
   
   /** Convenience function filtering to events which change this reactive to value */
   def changedTo(value: V) : Event[Unit] = changed && {_ == value} dropParam
   
   /** Return a Signal that gets updated only when e fires, and has the value of this Signal */
   def snapshot(e : Event[_]): Signal[V] = Signal.fold(e, self.getValue)((_,_) => self.getValue)
   
    /** Switch to (and keep) event value on occurrence of e*/
   def switchTo(e : Event[V]): Signal[V] = Signal.switchTo(e, self)
   
    /** Switch to (and keep) event value on occurrence of e*/
   def switchOnce(e : Event[_])(newSignal : Signal[V]): Signal[V] = Signal.switchOnce(e, self, newSignal)
   
   /** Switch back and forth between this and the other Signal on occurrence of event e */
   def toggle(e: Event[_])(other: Signal[V]) = Signal.toggle(e, self, other)

   /** Delays this event by n occurrences */
   def delay(n: Int) = Signal.delay(changed, n)
}

class Var[A](init: A) extends scalareact.Var[A](init, scalareact.owner) with ReactivityWrapper[A, A] {
    override def update(a: A) {
      super.update(a)
      scalareact.engine.runTurn
  }
}

object Var {
  def apply[A](init: A): Var[A] = new Var(init)
  
  // implicit conversion to a signal (Read-only)
  implicit def toSignal[A](v: Var[A]) = Signal { v() }
}

class Signal[A](op: => A) extends scalareact.StrictOpSignal[A](op) with ReactivityWrapper[A, A] {
  
  override def apply(): A = {
    try{ super.apply() }
    catch {
    	case e: Exception => System.err.println("You can not use apply out-of-turn. Make sure you did not call a signal expression, and use getValue!")
    	getValue
   	}
  }
}

object Signal {

  // creates a signal. Although it looks nicer, its a bad idea to make this implicit!
  def apply[T](op: => T): Signal[T] = {
    val signal = new Signal(op)
    scalareact.engine.runTurn
    signal
  }
  
  /** folds events with a given fold function to create a Signal */
  def fold[T,A](e : Event[T], init : A)(fold : (A,T) => A): Signal[A]  = {
	  val acc : Var[A] = Var(init)
	  e += {(newValue) =>
	   //scalareact.engine.runTurn
	   val old = acc.getValue
	   acc() = fold(old, newValue)
	   //scalareact.engine.runTurn
	  }
	  return acc
  }
  
  /** iterates a value on the occurrence of the event. */
  def iterate[A](e : Event[_], init : A)(f : A => A): Signal[A] = fold(e, init)((acc, _) => f(acc))
  
   /** calls f on each occurrence of event e, setting the Signal to the generated value */
  def set[T,A](e : Event[T], init : T)(f : T => A) : Signal[A] = fold(e, f(init))((_, v) => f(v))
  
  /** calls factory on each occurrence of event e, resetting the Signal to a newly generated one */
  def reset[T,A](e : Event[T], init : T)(factory : (T) => Signal[A]) : Signal[A] = {
    val ref: Signal[Signal[A]] = set(e, init)(factory)
    Signal { ref()() }
  }
  
  /** returns a signal holding the latest value of the event. */
  def latest[T](e : Event[T], init : T): Signal[T] = Signal.fold(e, init)((_,v) => v)

  /** Holds the latest value of an event as an Option, None before the first event occured */
  def latestOption[T](e : Event[T]) : Signal[Option[T]] = latest(e.map((x : T) => Some(x)), None)
   
  /** collects events resulting in a variable holding a list of all values. */
  def list[T](e : Event[T]): Signal[List[T]] = fold(e, List[T]())((acc, v) => v :: acc)
  
  /** returns a signal which holds the last n events */
  def last[T](e : Event[T], n : Int) : Signal[List[T]] = fold(e, List[T]())((acc, v) => (v :: acc).take(n)) 
  
  
   /** Switch to a new Signal once, on the occurrence of event e */
  def switchTo[T](e : Event[T], original:  scalareact.TotalReactive[_,T]): Signal[T] = {
    val latest = latestOption(e)
    Signal { latest() match {
      case None => original()
      case Some(x) => x
    }}
  }
  
  /** Switch to a new Signal once, on the occurrence of event e */
  def switchOnce[T](e: Event[_], original: scalareact.TotalReactive[_,T], newSignal: Signal[T]): Signal[T] = {
    val latest = latestOption(e)
    Signal { latest() match {
      case None => original()
      case Some(_) => newSignal()
    }}
  }
  
  /** Switch back and forth between two signals on occurrence of event e */
  def toggle[T](e : Event[_], a: scalareact.TotalReactive[_,T], b: scalareact.TotalReactive[_,T]): Signal[T] = {
    val switched = iterate(e, false) {! _}
    Signal { if(switched()) b() else a() }
  }
  
  /** Like latest, but delays the value of the resulting signal by n occurrences */
  def delay[T](e: Event[T], n: Int): Signal[T] = {
    val history = last(e, n)
    Signal { history().last }
  }
  
  /** Delays this signal by n occurrences */
  def delay[T](signal: Signal[T], n: Int): Signal[T] = delay(signal.changed, n)
  
   /** lifts a function A => B to work on scalareact reactives */
  def lift[A,B](f : A => B) : (scalareact.TotalReactive[A, A] => Signal[B]) = {
    return (a => Signal { f(a()) })
  }
  
}

object SignalConversions {
  
  // bad idea:
  implicit def toEvent[P,V](reactive: scalareact.Reactive[P, V]): Event[P] = 
    (new EventStreamWrapper(reactive)).event
    
  // this is dangerous: do we want expressions to be constant or changing??
  implicit def toVal[T](const: T): scalareact.Val[T] = scalareact.Val(const)
  implicit def toSignal[T](op: => T) : Signal[T] = Signal(op)
  
//
//  // This, sadly, does not work
//  implicit def lift[A,B](f : A => B) : (Signal[A] => Signal[B]) = {
//    return (a : Signal[A]) => Signal { f(a())}
//  }
//
//  /** wrap a constant into a Var */
//  implicit def toVar[T](const : T) : Var[T] = Var(const)
  
}