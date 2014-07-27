package rescala

import scala.collection.mutable.ListBuffer
import rescala.events.Event
import rescala.events.ChangedEventNode
import rescala.events.EventNode

//trait FixedDepHolder extends Reactive {
//  val fixedDependents = new ListBuffer[Dependent]
//  def addFixedDependent(dep: Dependent) = fixedDependents += dep
//  def removeFixedDependent(dep: Dependent) = fixedDependents -= dep
// def notifyDependents(change: Any): Unit = dependents.map(_.dependsOnchanged(change,this))
//}

/* A node that has nodes that depend on it */
class VarSynt[T](private[this] var value: T) extends Var[T] {

  def get = value

  def set(newValue: T): Unit = {
    if (value != newValue) {
      value = newValue
      TS.nextRound() // Testing
      timestamps += TS.newTs // testing

      notifyDependents(value)
      ReactiveEngine.startEvaluation()

    } else {
      ReactiveEngine.log.nodePropagationStopped(this)
      timestamps += TS.newTs // testing
    }
  }

  def reEvaluate(): T = value
}

object VarSynt {
  def apply[T](initialValue: T) = new VarSynt(initialValue)
}

trait DependentSignalImplementation[+T] extends DependentSignal[T] {

  def initialValue(): T
  def calculateNewValue(): T

  private[this] var currentValue = initialValue()

  def get = currentValue

  def triggerReevaluation() = reEvaluate()

  def reEvaluate(): T = {
    ReactiveEngine.log.nodeEvaluationStarted(this)

    timestamps += TS.newTs // Testing

    val oldLevel = level

     // Evaluation
    val newValue = calculateNewValue()

    /* if the level increses by one, the dependencies might or might not have been evaluated this turn.
     * if they have, we could just fire the observers, but if they have not we are not allowed to do so
     *
     * if the level increases by more than one, we depend on something that still has to be in the queue
     */
    if (level == oldLevel + 1) {
      ReactiveEngine.addToEvalQueue(this)
    }
    else {
      if (level <= oldLevel) {
        /* Notify dependents only of the value changed */
        if (currentValue != newValue) {
          currentValue = newValue
          timestamps += TS.newTs // Testing
          notifyDependents(currentValue)
        }
        else {
          ReactiveEngine.log.nodePropagationStopped(this)
          timestamps += TS.newTs // Testing
        }
      } : Unit
    }
    ReactiveEngine.log.nodeEvaluationEnded(this)
    newValue
  }
  override def dependsOnchanged(change: Any, dep: DepHolder) = {
    ReactiveEngine.addToEvalQueue(this)
  }

}

/** A dependant reactive value with dynamic dependencies (depending signals can change during evaluation) */
class SignalSynt[+T](reactivesDependsOnUpperBound: List[DepHolder])(expr: SignalSynt[T] => T)
  extends { private var detectedDependencies = Set[DepHolder]() } with DependentSignalImplementation[T] {

  // For glitch freedom
  if (reactivesDependsOnUpperBound.nonEmpty) ensureLevel(reactivesDependsOnUpperBound.map { _.level }.max)

  override def onDynamicDependencyUse[A](dependency: Signal[A]): Unit = {
    super.onDynamicDependencyUse(dependency)
    detectedDependencies += dependency
  }

  override def initialValue(): T = calculateNewValue()

  override def calculateNewValue(): T = {
    val newValue = expr(this)
    setDependOn(detectedDependencies)
    detectedDependencies = Set()
    newValue
  }

}

/**
 * A syntactic signal
 */
object SignalSynt {
  def apply[T](reactivesDependsOn: List[DepHolder])(expr: SignalSynt[T] => T) =
    new SignalSynt(reactivesDependsOn)(expr)

  def apply[T](expr: SignalSynt[T] => T): SignalSynt[T] = apply(List())(expr)
  def apply[T](dependencyHolders: DepHolder*)(expr: SignalSynt[T] => T): SignalSynt[T] = apply(dependencyHolders.toList)(expr)

}





/** A wrapped event inside a signal, that gets "flattened" to a plain event node */
class WrappedEvent[T](wrapper: Signal[Event[T]]) extends EventNode[T] with Dependent {
  
  var inQueue = false
  var currentEvent = wrapper.get
  var currentValue: T = _
  
  // statically depend on wrapper
  addDependOn(wrapper)
  // dynamically depend on inner event
  addDependOn(currentEvent)
  
  protected def updateProxyEvent(newEvent: Event[T]){
    // untie from from current event stream
    removeDependOn(currentEvent)
    // tie to new event stream
    addDependOn(newEvent)
    // remember current event
    currentEvent = newEvent
  }
  
  def triggerReevaluation() {
    timestamps += TS.newTs
    notifyDependents(currentValue)
    inQueue = false
  }
  
  override def dependsOnchanged(change: Any, dep: DepHolder) = {
    
    if(dep eq wrapper) { // wrapper changed the proxy event
	    val newEvent = change.asInstanceOf[Event[T]]
	    if(newEvent ne currentEvent)
	      updateProxyEvent(newEvent)
    }
    else if(dep eq currentEvent) { // proxied event changed
      // store the value to propagate
      currentValue = change.asInstanceOf[T]
      
      // and queue this node
      if (!inQueue) {
    	  inQueue = true
    	  ReactiveEngine.addToEvalQueue(this)
      }
    }
    else throw new IllegalStateException("Illegal DepHolder " + dep)

  }
  
  override val timestamps = ListBuffer[Stamp]()
}
