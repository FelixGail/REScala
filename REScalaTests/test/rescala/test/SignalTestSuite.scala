package rescala.test

import org.junit.{Before, Test}
import org.mockito.Mockito.verify
import org.scalatest.junit.AssertionsForJUnit
import org.scalatest.mock.MockitoSugar
import rescala._
import rescala.signals._

class SignalTestSuite extends AssertionsForJUnit with MockitoSugar {

  var dh: Dependency = _
  var v:  Var[Int]  = _
  var s1: DependentSignal[Int] = _
  var s2: DependentSignal[Int] = _
  var s3: DependentSignal[Int] = _

  @Before def initialize(): Unit = {}

  @Test def dependencyHolderNotifiesDependentsWhenNotifyDependentsIsCalled(): Unit = {

    dh = new {} with Dependency {}
    v  = Var(0)
    s1 = mock[DependentSignal[Int]]
    s2 = mock[DependentSignal[Int]]
    s3 = mock[DependentSignal[Int]]

    dh.addDependant(s1)
    dh.addDependant(s2)
    dh.addDependant(s3)
    dh.notifyDependants({})

    verify(s1).dependencyChanged({},dh)
    verify(s2).dependencyChanged({},dh)
    verify(s3).dependencyChanged({},dh)

  }

  @Test def signalReEvaluatesTheExpression(): Unit = {
    v  = Var(0)
    var i = 1
    var s: Signal[Int] = StaticSignal[Int](v) { i }
    i = 2
    v.set(2)
    assert(s.get == 2)
  }

  @Test def theExpressionIsNoteEvaluatedEveryTimeGetValIsCalled(): Unit = {
    var a = 10
    var s: Signal[Int] = StaticSignal(List())( 1 + 1 + a )
    assert(s.get === 12)
    a = 11
    assert(s.get === 12)
  }


  @Test def simpleSignalReturnsCorrectExpressions(): Unit = {
    var s: Signal[Int] = StaticSignal(List())( 1 + 1 + 1 )
    assert(s.get === 3)
  }

  @Test def theExpressionIsEvaluatedOnlyOnce(): Unit = {

    var a = 0
    val v = Var(10)
    var s1: Signal[Int] = StaticSignal(v){ a +=1; v.get % 10 }
    var s2: Signal[Int] = StaticSignal(s1){ a }


    assert(a == 1)
    v.set(11)
    assert(a == 2)
    v.set(21)
    assert(a == 3)
  }

  @Test def handlersAreExecuted() =  {

    var test = 0
    v = Var(1)

    s1 = StaticSignal(v){ 2 * v.get }
    s2 = StaticSignal(v){ 3 * v.get }
    s3 = StaticSignal(s1,s2){ s1.get + s2.get }

    s1.changed += { (_) => test += 1 }
    s2.changed += { (_) => test += 1 }
    s3.changed += { (_) => test += 1 }

    assert(test == 0)

    v.set(3)
    assert(test == 3)

  }

  @Test def levelIsCorrectlyComputed() =  {

    var test = 0
    v = Var(1)

    s1 = StaticSignal(v){ 2 * v.get }
    s2 = StaticSignal(v){ 3 * v.get }
    s3 = StaticSignal(s1,s2){ s1.get + s2.get }

    assert(v.level == 0)
    assert(s1.level == 1)
    assert(s2.level == 1)
    assert(s3.level == 2)
  }

}
