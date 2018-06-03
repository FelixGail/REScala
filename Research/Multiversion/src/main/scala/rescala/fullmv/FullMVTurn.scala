package rescala.fullmv

import java.util
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.{LockSupport, ReentrantReadWriteLock}
import java.util.concurrent.{ConcurrentHashMap, ThreadLocalRandom}

import rescala.core.Initializer.InitValues
import rescala.core._
import rescala.fullmv.NotificationResultAction._
import rescala.fullmv.NotificationResultAction.NotificationOutAndSuccessorOperation._
import rescala.fullmv.tasks.{FullMVAction, Notification, Reevaluation}

import scala.annotation.tailrec

class FullMVTurn(val engine: FullMVEngine, val userlandThread: Thread) extends Initializer[FullMVStruct] {
  var initialChanges: collection.Map[ReSource[FullMVStruct], InitialChange[FullMVStruct]] = _

  // ===== Turn State Manangement External API
  val externallyPushedTasks = new AtomicReference[List[FullMVAction]](Nil)
  val localTaskQueue = new util.ArrayDeque[FullMVAction]()
  def pushExternalTask(action: FullMVAction): Unit = {
    assert(action.turn == this, s"$this received task of different turn: $action")
    @inline @tailrec def retryAdd(): Unit = {
      val before = externallyPushedTasks.get
      val update = action :: before
      if(!externallyPushedTasks.compareAndSet(before, update)) retryAdd()
    }
    retryAdd()
  }
  def pushLocalTask(action: FullMVAction): Unit = {
    assert(action.turn == this, s"$this received task of different turn: $action")
    localTaskQueue.addLast(action)
  }
  val waiters = new ConcurrentHashMap[Thread, TurnPhase.Type]()
  def wakeWaitersAfterPhaseSwitch(newPhase: TurnPhase.Type): Unit = {
    val it = waiters.entrySet().iterator()
    while (it.hasNext) {
      val waiter = it.next()
      if (FullMVEngine.DEBUG) println(s"[${Thread.currentThread().getName}] $this phase switch unparking ${waiter.getKey.getName}.")
      if (waiter.getValue <= newPhase) LockSupport.unpark(waiter.getKey)
    }
  }

  private val hc = ThreadLocalRandom.current().nextInt()
  override def hashCode(): Int = hc

  val phaseLock = new ReentrantReadWriteLock()
  @volatile var phase: TurnPhase.Type = TurnPhase.Uninitialized

  var successorsIncludingSelf: List[FullMVTurn] = this :: Nil // this is implicitly a set
  @volatile var selfNode = new MutableTransactionSpanningTreeNode[FullMVTurn](this) // this is also implicitly a set
  @volatile var predecessorSpanningTreeNodes: Map[FullMVTurn, MutableTransactionSpanningTreeNode[FullMVTurn]] = new scala.collection.immutable.Map.Map1(this, selfNode)

  //========================================================Local State Control============================================================

//  var spinSwitch = 0
//  var parkSwitch = 0
//  val spinRestart = new java.util.HashSet[String]()
//  val parkRestart = new java.util.HashSet[String]()

  @tailrec private def runLocalQueue(): Unit = {
    val head = localTaskQueue.pollFirst()
    if(head != null) {
      assert(head.turn == this, s"local queue of $this contains different turn's $head")
      head.compute()
      runLocalQueue()
    }
  }

  private def awaitAndSwitchPhase(newPhase: TurnPhase.Type): Unit = {
    assert(newPhase > this.phase, s"$this cannot progress backwards to phase $newPhase.")
    runLocalQueue()
    @inline @tailrec def awaitAndSwitchPhase0(firstUnknownPredecessorIndex: Int, parkAfter: Long, registeredForWaiting: FullMVTurn): Unit = {
      if (externallyPushedTasks.get.nonEmpty) {
        if (registeredForWaiting != null) {
          registeredForWaiting.waiters.remove(this.userlandThread)
//          parkRestart.add(head.asInstanceOf[ReevaluationResultHandling[ReSource[FullMVStruct]]].node.toString)
//        } else if (parkAfter > 0) {
//          spinRestart.add(head.asInstanceOf[ReevaluationResultHandling[ReSource[FullMVStruct]]].node.toString)
        }
        val newTasksFromExternal = externallyPushedTasks.getAndSet(Nil).iterator
        if (FullMVEngine.DEBUG) println(s"[${Thread.currentThread().getName}] $this ingesting externally pushed tasks $newTasksFromExternal.")
        while(newTasksFromExternal.hasNext) pushLocalTask(newTasksFromExternal.next())
        runLocalQueue()
        awaitAndSwitchPhase0(firstUnknownPredecessorIndex, 0L, null)
      } else if (firstUnknownPredecessorIndex == selfNode.size) {
        assert(registeredForWaiting == null, s"$this is still registered on $registeredForWaiting as waiter despite having finished waiting for it")
        phaseLock.writeLock.lock()
        // make thread-safe sure that we haven't received any new predecessors that might
        // not be in the next phase yet. Only once that's sure we can also thread-safe sure
        // check that no predecessors pushed any tasks into our queue anymore. And only then
        // can we phase switch.
        if (firstUnknownPredecessorIndex == selfNode.size && externallyPushedTasks.get.isEmpty) {
          this.phase = newPhase
          phaseLock.writeLock.unlock()
          val it = waiters.entrySet().iterator()
          while (it.hasNext) {
            val waiter = it.next()
            if (FullMVEngine.DEBUG) println(s"[${Thread.currentThread().getName}] ${FullMVTurn.this} phase switch unparking ${waiter.getKey.getName}.")
            if (waiter.getValue <= newPhase) LockSupport.unpark(waiter.getKey)
          }
          if (FullMVEngine.DEBUG) println(s"[${Thread.currentThread().getName}] $this switched phase.")
        } else {
          phaseLock.writeLock.unlock()
          awaitAndSwitchPhase0(firstUnknownPredecessorIndex, 0L, null)
        }
      } else {
        val currentUnknownPredecessor = selfNode.children(firstUnknownPredecessorIndex).txn
        if(currentUnknownPredecessor.phase < newPhase) {
          if (registeredForWaiting != null) {
            if (FullMVEngine.DEBUG) println(s"[${Thread.currentThread().getName}] $this parking for $currentUnknownPredecessor.")
            val timeBefore = System.nanoTime()
            LockSupport.parkNanos(currentUnknownPredecessor, 1000000000L)
            val timeElapsed = System.nanoTime() - timeBefore
            if(timeElapsed > 500000000L) {
              System.err.println(if(externallyPushedTasks.get.isEmpty && currentUnknownPredecessor.phase < newPhase) {
                s"${Thread.currentThread().getName} stalled waiting for transition to ${TurnPhase.toString(newPhase)} of $currentUnknownPredecessor"
              } else {
                s"${Thread.currentThread().getName} stalled due do missing wake-up after transition to ${TurnPhase.toString(newPhase)} of $currentUnknownPredecessor"
              })
            }
            if (FullMVEngine.DEBUG) println(s"[${Thread.currentThread().getName}] $this unparked with external queue ${externallyPushedTasks.get}.")
            awaitAndSwitchPhase0(firstUnknownPredecessorIndex, 0L, currentUnknownPredecessor)
          } else {
            val now = System.nanoTime()
            val parkTimeSet = parkAfter > 0L
            if(parkTimeSet && now > parkAfter) {
              currentUnknownPredecessor.waiters.put(this.userlandThread, newPhase)
              awaitAndSwitchPhase0(firstUnknownPredecessorIndex, 0L, currentUnknownPredecessor)
            } else {
              val end = now + FullMVTurn.CONSTANT_BACKOFF
              do {
                Thread.`yield`()
              } while (System.nanoTime() < end)
              awaitAndSwitchPhase0(firstUnknownPredecessorIndex, if(parkTimeSet) parkAfter else now + FullMVTurn.MAX_BACKOFF, null)
            }
          }
        } else {
          if (registeredForWaiting != null) {
            currentUnknownPredecessor.waiters.remove(this.userlandThread)
//            parkSwitch += 1
//          } else if (parkAfter > 0) {
//            spinSwitch += 1
          }
          awaitAndSwitchPhase0(firstUnknownPredecessorIndex + 1, 0L, null)
        }
      }
    }
    awaitAndSwitchPhase0(0, 0L, null)
  }

  private def beginPhase(phase: TurnPhase.Type): Unit = {
    assert(this.phase == TurnPhase.Uninitialized, s"$this already begun")
    assert(localTaskQueue.isEmpty, s"$this cannot begin $phase: queue non empty!")
    assert(externallyPushedTasks.get == Nil, s"$this cannot begin $phase: external queue non empty!")
    assert(selfNode.size == 0, s"$this cannot begin $phase: already has predecessors!")
    if (FullMVEngine.DEBUG) println(s"[${Thread.currentThread().getName}] $this begun.")
    this.phase = phase
  }

  def beginFraming(): Unit = beginPhase(TurnPhase.Framing)
  def beginExecuting(): Unit = beginPhase(TurnPhase.Executing)

//  def resetStatistics() = {
//    spinSwitch = 0
//    parkSwitch = 0
//    spinRestart.clear()
//    parkRestart.clear()
//  }

  def completeFraming(): Unit = {
    assert(this.phase == TurnPhase.Framing, s"$this cannot complete framing: Not in framing phase")
//    resetStatistics()
    awaitAndSwitchPhase(TurnPhase.Executing)
//    FullMVTurn.framesync.synchronized {
//      val maybeCount1 = FullMVTurn.spinSwitchStatsFraming.get(spinSwitch)
//      FullMVTurn.spinSwitchStatsFraming.put(spinSwitch, if(maybeCount1 == null) 1L else maybeCount1 + 1L)
//      val maybeCount2 = FullMVTurn.parkSwitchStatsFraming.get(parkSwitch)
//      FullMVTurn.parkSwitchStatsFraming.put(parkSwitch, if(maybeCount2 == null) 1L else maybeCount2 + 1L)
//      val it1 = spinRestart.iterator()
//      while(it1.hasNext) {
//        val key = it1.next()
//        val maybeCount3 = FullMVTurn.spinRestartStatsFraming.get(key)
//        FullMVTurn.spinRestartStatsFraming.put(key, if (maybeCount3 == null) 1L else maybeCount3 + 1L)
//      }
//      val it2 = parkRestart.iterator()
//      while(it2.hasNext) {
//        val key = it2.next()
//        val maybeCount4 = FullMVTurn.parkRestartStatsFraming.get(key)
//        FullMVTurn.parkRestartStatsFraming.put(key, if (maybeCount4 == null) 1L else maybeCount4 + 1L)
//      }
//    }
  }
  def completeExecuting(): Unit = {
    assert(this.phase == TurnPhase.Executing, s"$this cannot complete executing: Not in executing phase")
//    resetStatistics()
    awaitAndSwitchPhase(TurnPhase.Completed)
    predecessorSpanningTreeNodes = Map.empty
    selfNode = null
//    FullMVTurn.execsync.synchronized {
//      val maybeCount1 = FullMVTurn.spinSwitchStatsExecuting.get(spinSwitch)
//      FullMVTurn.spinSwitchStatsExecuting.put(spinSwitch, if(maybeCount1 == null) 1L else maybeCount1 + 1L)
//      val maybeCount2 = FullMVTurn.parkSwitchStatsExecuting.get(parkSwitch)
//      FullMVTurn.parkSwitchStatsExecuting.put(parkSwitch, if(maybeCount2 == null) 1L else maybeCount2 + 1L)
//      val it1 = spinRestart.iterator()
//      while(it1.hasNext) {
//        val key = it1.next()
//        val maybeCount3 = FullMVTurn.spinRestartStatsExecuting.get(key)
//        FullMVTurn.spinRestartStatsExecuting.put(key, if (maybeCount3 == null) 1L else maybeCount3 + 1L)
//      }
//      val it2 = parkRestart.iterator()
//      while(it2.hasNext) {
//        val key = it2.next()
//        val maybeCount4 = FullMVTurn.parkRestartStatsExecuting.get(key)
//        FullMVTurn.parkRestartStatsExecuting.put(key, if (maybeCount4 == null) 1L else maybeCount4 + 1L)
//      }
//    }
  }

  //========================================================Ordering Search and Establishment Interface============================================================

  def isTransitivePredecessor(txn: FullMVTurn): Boolean = {
    predecessorSpanningTreeNodes.contains(txn)
  }

  def acquirePhaseLockIfAtMost(maxPhase: TurnPhase.Type): TurnPhase.Type = {
    val pOptimistic = phase
    if(pOptimistic > maxPhase) {
      pOptimistic
    } else {
      phaseLock.readLock().lock()
      val pSecure = phase
      if (pSecure > maxPhase) phaseLock.readLock().unlock()
      pSecure
    }
  }

  def addPredecessor(predecessorSpanningTree: TransactionSpanningTreeNode[FullMVTurn]): Unit = {
    assert(engine.lock.isHeldByCurrentThread, s"addPredecessor by thread that doesn't hold the SGT lock")
    assert(!isTransitivePredecessor(predecessorSpanningTree.txn), s"attempted to establish already existing predecessor relation ${predecessorSpanningTree.txn} -> $this")
    if(FullMVEngine.DEBUG) println(s"[${Thread.currentThread().getName}] $this new predecessor ${predecessorSpanningTree.txn}.")
    for(succ <- successorsIncludingSelf) succ.maybeNewReachableSubtree(this, predecessorSpanningTree)
  }

  def maybeNewReachableSubtree(attachBelow: FullMVTurn, spanningSubTreeRoot: TransactionSpanningTreeNode[FullMVTurn]): Unit = {
    if (!isTransitivePredecessor(spanningSubTreeRoot.txn)) {
      predecessorSpanningTreeNodes = copySubTreeRootAndAssessChildren(predecessorSpanningTreeNodes, attachBelow, spanningSubTreeRoot)
    }
  }

  private def copySubTreeRootAndAssessChildren(bufferPredecessorSpanningTreeNodes: Map[FullMVTurn, MutableTransactionSpanningTreeNode[FullMVTurn]], attachBelow: FullMVTurn, spanningSubTreeRoot: TransactionSpanningTreeNode[FullMVTurn]): Map[FullMVTurn, MutableTransactionSpanningTreeNode[FullMVTurn]] = {
    val newTransitivePredecessor = spanningSubTreeRoot.txn
    // last chance to check if predecessor completed concurrently
    if(newTransitivePredecessor.phase != TurnPhase.Completed) {
      newTransitivePredecessor.newSuccessor(this)
      val copiedSpanningTreeNode = new MutableTransactionSpanningTreeNode(newTransitivePredecessor)
      var updatedBufferPredecessorSpanningTreeNodes = bufferPredecessorSpanningTreeNodes + (newTransitivePredecessor -> copiedSpanningTreeNode)
      updatedBufferPredecessorSpanningTreeNodes(attachBelow).addChild(copiedSpanningTreeNode)

      val it = spanningSubTreeRoot.iterator()
      while (it.hasNext) {
        val child = it.next()
        if (!isTransitivePredecessor(child.txn)) {
          val updated2Buffer = copySubTreeRootAndAssessChildren(updatedBufferPredecessorSpanningTreeNodes, newTransitivePredecessor, child)
          updatedBufferPredecessorSpanningTreeNodes = updated2Buffer
        }
      }
      updatedBufferPredecessorSpanningTreeNodes
    } else {
      bufferPredecessorSpanningTreeNodes
    }
  }

  def newSuccessor(successor: FullMVTurn): Unit = successorsIncludingSelf = successor :: successorsIncludingSelf

  def asyncReleasePhaseLock(): Unit = phaseLock.readLock().unlock()

  //========================================================ToString============================================================

  override def toString: String = s"FullMVTurn($hc by ${if(userlandThread == null) "none" else userlandThread.getName}, ${TurnPhase.toString(phase)}${if(!localTaskQueue.isEmpty || externallyPushedTasks.get.nonEmpty) s"(${localTaskQueue.size()}+${externallyPushedTasks.get.size})" else ""})"

  //========================================================Scheduler Interface============================================================

  override def makeDerivedStructState[P](valuePersistency: InitValues[P]): NonblockingSkipListVersionHistory[P, FullMVTurn, ReSource[FullMVStruct], Reactive[FullMVStruct]] = {
    val state = new NonblockingSkipListVersionHistory[P, FullMVTurn, ReSource[FullMVStruct], Reactive[FullMVStruct]](engine.dummy, valuePersistency)
    state.incrementFrame(this)
    state
  }

  override protected def makeSourceStructState[P](valuePersistency: InitValues[P]): NonblockingSkipListVersionHistory[P, FullMVTurn, ReSource[FullMVStruct], Reactive[FullMVStruct]] = {
    val state = makeDerivedStructState(valuePersistency)
    val res = state.notify(this, changed = false)
    assert(res == NoSuccessor(Set.empty))
    state
  }

  override def ignite(reactive: Reactive[FullMVStruct], incoming: Set[ReSource[FullMVStruct]], ignitionRequiresReevaluation: Boolean): Unit = {
    assert(Thread.currentThread() == userlandThread, s"$this ignition of $reactive on different thread ${Thread.currentThread().getName}")
//    assert(Thread.currentThread() == userlandThread, s"$this ignition of $reactive on different thread ${Thread.currentThread().getName}")
    if (FullMVEngine.DEBUG) println(s"[${Thread.currentThread().getName}] $this igniting $reactive on $incoming")
    incoming.foreach { discover =>
      discover.state.dynamicAfter(this) // TODO should we get rid of this?
    val (successorWrittenVersions, maybeFollowFrame) = discover.state.discover(this, reactive)
      reactive.state.retrofitSinkFrames(successorWrittenVersions, maybeFollowFrame, 1)
    }
    reactive.state.incomings = incoming
    // Execute this notification manually to be able to execute a resulting reevaluation immediately.
    // Subsequent reevaluations from retrofitting will be added to the global pool, but not awaited.
    // This matches the required behavior where the code that creates this reactive is expecting the initial
    // reevaluation (if one is required) to have been completed, but cannot access values from subsequent turns
    // and hence does not need to wait for those.
    val ignitionNotification = Notification(this, reactive, changed = ignitionRequiresReevaluation)
    ignitionNotification.deliverNotification() match {
      case NotGlitchFreeReady =>
        if (FullMVEngine.DEBUG) println(s"[${Thread.currentThread().getName}] $this ignite $reactive did not spawn reevaluation.")
      // ignore
      case GlitchFreeReady =>
        if (FullMVEngine.DEBUG) println(s"[${Thread.currentThread().getName}] $this ignite $reactive spawned reevaluation.")
        Reevaluation(this, reactive).compute()
      case NextReevaluation(out, succTxn) if out.isEmpty =>
        if (FullMVEngine.DEBUG) println(s"[${Thread.currentThread().getName}] $this ignite $reactive spawned reevaluation for successor $succTxn.")
        succTxn.pushExternalTask(Reevaluation(succTxn, reactive))
        LockSupport.unpark(succTxn.userlandThread)
      case otherOut: NotificationOutAndSuccessorOperation[FullMVTurn, Reactive[FullMVStruct]] if otherOut.out.isEmpty =>
        // ignore
      case other =>
        throw new AssertionError(s"$this ignite $reactive: unexpected result: $other")
    }
  }

  def discover(node: ReSource[FullMVStruct], addOutgoing: Reactive[FullMVStruct]): Unit = {
    val r@(successorWrittenVersions, maybeFollowFrame) = node.state.discover(this, addOutgoing)
    assert((successorWrittenVersions ++ maybeFollowFrame).forall(retrofit => retrofit == this || retrofit.isTransitivePredecessor(this)), s"$this retrofitting contains predecessors: discover $node -> $addOutgoing retrofits $r from ${node.state}")
    if (FullMVEngine.DEBUG) println(s"[${Thread.currentThread().getName}] Reevaluation($this,$addOutgoing) discovering $node -> $addOutgoing re-queueing $successorWrittenVersions and re-framing $maybeFollowFrame")
    addOutgoing.state.retrofitSinkFrames(successorWrittenVersions, maybeFollowFrame, 1)
  }

  def drop(node: ReSource[FullMVStruct], removeOutgoing: Reactive[FullMVStruct]): Unit = {
    val r@(successorWrittenVersions, maybeFollowFrame) = node.state.drop(this, removeOutgoing)
    assert((successorWrittenVersions ++ maybeFollowFrame).forall(retrofit => retrofit == this || retrofit.isTransitivePredecessor(this)), s"$this retrofitting contains predecessors: drop $node -> $removeOutgoing retrofits $r from ${node.state}")
    if (FullMVEngine.DEBUG) println(s"[${Thread.currentThread().getName}] Reevaluation($this,$removeOutgoing) dropping $node -> $removeOutgoing de-queueing $successorWrittenVersions and de-framing $maybeFollowFrame")
    removeOutgoing.state.retrofitSinkFrames(successorWrittenVersions, maybeFollowFrame, -1)
  }

  private[rescala] def writeIndeps(node: Reactive[FullMVStruct], indepsAfter: Set[ReSource[FullMVStruct]]): Unit = node.state.incomings = indepsAfter

  private[rescala] def staticBefore(reactive: ReSource[FullMVStruct]) = reactive.state.staticBefore(this)
  private[rescala] def staticAfter(reactive: ReSource[FullMVStruct]) = reactive.state.staticAfter(this)
  private[rescala] def dynamicBefore(reactive: ReSource[FullMVStruct]) = reactive.state.dynamicBefore(this)
  private[rescala] def dynamicAfter(reactive: ReSource[FullMVStruct]) = reactive.state.dynamicAfter(this)

  def observe(f: () => Unit): Unit = f()
}

object FullMVTurn {
  val CONSTANT_BACKOFF = 7500L // 7.5µs
  val MAX_BACKOFF = 100000L // 100µs

//  object framesync
//  var spinSwitchStatsFraming = new java.util.HashMap[Int, java.lang.Long]()
//  var parkSwitchStatsFraming = new java.util.HashMap[Int, java.lang.Long]()
//  val spinRestartStatsFraming = new java.util.HashMap[String, java.lang.Long]()
//  val parkRestartStatsFraming =  new java.util.HashMap[String, java.lang.Long]()
//  object execsync
//  var spinSwitchStatsExecuting = new java.util.HashMap[Int, java.lang.Long]()
//  var parkSwitchStatsExecuting = new java.util.HashMap[Int, java.lang.Long]()
//  val spinRestartStatsExecuting = new java.util.HashMap[String, java.lang.Long]()
//  val parkRestartStatsExecuting = new java.util.HashMap[String, java.lang.Long]()
}
