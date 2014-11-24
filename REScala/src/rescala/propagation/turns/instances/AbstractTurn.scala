package rescala.propagation.turns.instances

import rescala.propagation.EvaluationResult.{Dynamic, Static}
import rescala.propagation.Reactive
import rescala.propagation.turns.Turn

import scala.annotation.tailrec
import scala.collection.mutable
import scala.util.DynamicVariable

abstract class AbstractTurn extends Turn {

  protected val evalQueue = new mutable.PriorityQueue[(Int, Reactive)]()(Synchronized.reactiveOrdering)
  protected var toCommit = Set[Reactive]()
  protected var afterCommitHandlers = List[() => Unit]()

  implicit def implicitThis: Turn = this

  def register(dependant: Reactive, dependencies: Set[Reactive]): Unit = {
    dependencies.foreach { dependency =>
      dependency.dependants.transform(_ + dependant)
      markForCommit(dependency)
    }
    ensureLevel(dependant, dependencies)
  }

  def ensureLevel(dependant: Reactive, dependencies: Set[Reactive]): Unit =
    if (dependencies.nonEmpty) {
      if (setLevelIfHigher(dependant, dependencies.map(_.level.get).max + 1)) {
        markForCommit(dependant)
      }
    }

  def setLevelIfHigher(reactive: Reactive, level: Int): Boolean = {
    reactive.level.transform { case x if x < level => level }
  }

  override def unregister(dependant: Reactive)(dependency: Reactive): Unit = {
    acquireDynamic(dependency)
    dependency.dependants.transform(_ - dependant)
    markForCommit(dependency)
  }

  def handleDiff(dependant: Reactive,newDependencies: Set[Reactive] , oldDependencies: Set[Reactive]): Unit = {
    newDependencies.foreach(acquireDynamic)
    oldDependencies.diff(newDependencies).foreach(unregister(dependant))
    register(dependant, newDependencies.diff(oldDependencies))
  }


  def isReady(reactive: Reactive, dependencies: Set[Reactive]) =
    dependencies.forall(_.level.get < reactive.level.get)

  @tailrec
  protected final def floodLevel(reactives: Set[Reactive]): Unit =
    if (reactives.nonEmpty) {
      val reactive = reactives.head
      markForCommit(reactive)
      val level = reactive.level.get + 1
      val dependants = reactive.dependants.get
      val changedDependants = dependants.filter(setLevelIfHigher(_, level))
      floodLevel(reactives.tail ++ changedDependants)
    }

  /** Adds a dependant to the eval queue */
  override def enqueue(dep: Reactive): Unit = {
    if (!evalQueue.exists { case (_, elem) => elem eq dep }) {
      evalQueue.+=((dep.level.get, dep))
    }
  }


  /** evaluates a single reactive */
  def evaluate(head: Reactive): Unit = {
    val result = head.reevaluate()(this)
    val headChanged = result match {
      case Static(hasChanged) => hasChanged
      case diff@Dynamic(hasChanged, newDependencies, oldDependencies) =>
        handleDiff(head, newDependencies, oldDependencies)
        if (isReady(head, newDependencies)) {
          floodLevel(Set(head))
          hasChanged
        }
        else {
          enqueue(head)
          false
        }
    }
    if (headChanged) {
      markForCommit(head)
      head.dependants.get.foreach(enqueue)
    }

  }

  /** Evaluates all the elements in the queue */
  def evaluateQueue() = {
    while (evalQueue.nonEmpty) {
      val (level, head) = evalQueue.dequeue()
      // check the level if it changed queue again
      if (level != head.level.get) enqueue(head)
      else evaluate(head)
    }
  }

  def markForCommit(reactive: Reactive): Unit = {
    toCommit += reactive
  }

  def commit() = toCommit.foreach(_.commit(this))

  override def afterCommit(handler: => Unit) = afterCommitHandlers ::= handler _

  def runAfterCommitHandlers() = afterCommitHandlers.foreach(_())

  val bag = new DynamicVariable(Set[Reactive]())
  override def collectDependencies[T](f: => T): (T, Set[Reactive]) = bag.withValue(Set()) { (f, bag.value) }
  override def useDependency(dependency: Reactive): Unit = bag.value = bag.value + dependency

  override def create[T <: Reactive](dependencies: Set[Reactive])(f: => T): T

  override def createDynamic[T <: Reactive](dependencies: Set[Reactive])(f: => T): T

  def acquireDynamic(reactive: Reactive): Unit

  /** admits a new source change */
  override def admit(source: Reactive)(setPulse: => Boolean): Unit = if(setPulse) enqueue(source)
}