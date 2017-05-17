package rescala.levelbased

import rescala.engine.{Engine, Turn, ValuePersistency}
import rescala.graph.Struct
import rescala.twoversion.TwoVersionEngineImpl

import scala.language.existentials

/**
  * Basic implementations of propagation engines
  */
trait LevelBasedPropagationEngines {

  type TEngine = Engine[S, Turn[S]] forSome { type S <: Struct }

  private[rescala] class SimpleNoLock extends LevelBasedPropagation[SimpleStruct] {
    override protected def makeStructState[P](valuePersistency: ValuePersistency[P]): SimpleStruct#State[P, SimpleStruct] = {
      new LevelStructTypeImpl(valuePersistency.initialValue, valuePersistency.isTransient)
    }
    override def releasePhase(): Unit = ()
  }

  type SimpleEngine = Engine[SimpleStruct, LevelBasedPropagation[SimpleStruct]]


  implicit val synchron: SimpleEngine = new TwoVersionEngineImpl[SimpleStruct, SimpleNoLock]("Synchron", new SimpleNoLock()) {
    override protected[rescala] def executeTurn[I, R](initialWrites: Traversable[Reactive], admissionPhase: SimpleNoLock => I, wrapUpPhase: (I, SimpleNoLock) => R): R = synchronized(super.executeTurn(initialWrites, admissionPhase, wrapUpPhase))
  }

  implicit val unmanaged: SimpleEngine = new TwoVersionEngineImpl[SimpleStruct, SimpleNoLock]("Unmanaged", new SimpleNoLock())

}

object LevelBasedPropagationEngines extends LevelBasedPropagationEngines
