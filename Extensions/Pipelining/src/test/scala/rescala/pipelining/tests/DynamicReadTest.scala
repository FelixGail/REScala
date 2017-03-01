package rescala.pipelining.tests

import org.scalatest.FlatSpec
import rescala.pipelining.PipelineEngine
import rescala.pipelining.tests.PipelineTestUtils._
import rescala.pipelining.util.LogUtils
import rescala.reactives.{Signals, Var}

/**
  * @author moritzlichter
  */
class DynamicReadTest extends FlatSpec {

  trait PipelineState {

    implicit val engine = new PipelineEngine()

    val minEvaluationTimeOfUpdate = 500l
    val letOtherUpdateCreateFramesTime = 200l

    assert(letOtherUpdateCreateFramesTime < minEvaluationTimeOfUpdate) // Such that the frames are still there
    LogUtils.log("")
    LogUtils.log("")

    val source1 = Var(0)
    val source2 = Var(100)

    val dynamicDep = engine.Signal {
      LogUtils.log(s"Evaluate base ${source1()}"): @unchecked
      if (source1() % 2 != 0)
        source2()
      else 0
    }

    val depOfDynamic = Signals.static(dynamicDep)(implicit t => {
      Thread.sleep(minEvaluationTimeOfUpdate)
      LogUtils.log(s"$t: Eval of dyn dep completed")
      dynamicDep.pulse.get + 1
    })

    val source2Dep = Signals.static(source2)(implicit t => {
      Thread.sleep(minEvaluationTimeOfUpdate)
      LogUtils.log(s"$t: Eval of source2 dep completed")
      source2.pulse.get + 1
    })

    /*
   * Initial dependency graph
   *
   * source2          source1
   *   |                |
   *   |                v
   *   |           dynamicDep
   *   |                |
   *   v                v
   * source2Dep    depOfDynamic
   *
   *
   */

    /*
   * If the value of source1 is odd, we have
   *
   * source2          source1
   *   |    \           |
   *   |     \          v
   *   |      \---> dynamicDep
   *   |                |
   *   v                v
   * source2Dep    depOfDynamic
   *
   */

    // Track the value
    val depOfDynamicTracker = new ValueTracker(depOfDynamic)
    val source2DepTracker = new ValueTracker(source2Dep)
  }

  it should "add Dynamic Dependency1 Before2" in new PipelineState {
    LogUtils.log("======")
    source1.set(1)
    assert(depOfDynamic.now == 101)
    source2.set(200)
    assert(depOfDynamic.now == 201)

    assert(depOfDynamicTracker.values == List(101, 201))
    assert(source2DepTracker.values == List(201))
  }

  it should "add Dynamic Dependency2 Before1" in new PipelineState {
    LogUtils.log("======")
    source2.set(200)
    assert(depOfDynamic.now == 1)
    source1.set(1)
    assert(depOfDynamic.now == 201)

    assert(depOfDynamicTracker.values == List(201))
    assert(source2DepTracker.values == List(201))
  }

  it should "addDynamicDependencyParallel2Before1()" in new PipelineState {


    val thread1 = createThread {Thread.sleep(2 * letOtherUpdateCreateFramesTime); source1.set(1)}
    val thread2 = createThread {source2.set(200)}

    LogUtils.log("=======")

    thread2.start
    thread1.start
    thread1.join
    thread2.join

    // In any case
    assert(source2DepTracker.values == List(201))

    assert(depOfDynamicTracker.values == List(201))

  }

  it should "addDynamicDependencyParallel1Before2" in new PipelineState {


    val thread1 = createThread {source1.set(1)}
    val thread2 = createThread {Thread.sleep(letOtherUpdateCreateFramesTime); source2.set(200)}

    LogUtils.log("====")
    thread2.start
    thread1.start
    thread1.join
    thread2.join

    // In any case
    assert(source2DepTracker.values == List(201))

    // if the change at 1 is before 2, the update at 2 is visible at depOfDynamicDep,
    // otherwise only the end result
    assert(depOfDynamicTracker.values == List(101, 201))

  }



  it should "exisiting Turns After Dynamic Propagate To New Nodes" in new PipelineState {
    // Need to enforce an order between turns that update
    // source1 and source2 when the dynamic dependency has
    // not been established
    // To do that, I connect the paths with a new node
    val source1b = Var(0)
    val source2b = Var(0)
    val dep1 = Signals.static(source1b)(implicit t => {Thread.sleep(minEvaluationTimeOfUpdate); source1b.pulse.get})
    val dynDep1 = engine.Signal { if (dep1() % 2 == 0) dep1() else dep1() + source2b() }
    val dep2 = Signals.static(source2b)(implicit t => source2b.pulse.get)
    val dep12 = Signals.static(dynDep1, dep2)(implicit t => dynDep1.pulse.get + dep2.pulse.get)

    val dep1Tracker = new ValueTracker(dep1)
    val dynDep1Tracker = new ValueTracker(dynDep1)
    val dep2Tracker = new ValueTracker(dep2)
    val dep12Tracker = new ValueTracker(dep12)

    val thread1 = createThread {source1b.set(1)}
    val thread2 = createThread {Thread.sleep(letOtherUpdateCreateFramesTime); source2b.set(100)}

    LogUtils.log("=======")

    thread1.start
    thread2.start
    thread1.join
    thread2.join

    assert(dep1Tracker.values == List(1))
    assert(dynDep1Tracker.values == List(1, 101))
    assert(dep2Tracker.values == List(100))
    assert(dep12Tracker.values == List(1, 201))

  }

}
