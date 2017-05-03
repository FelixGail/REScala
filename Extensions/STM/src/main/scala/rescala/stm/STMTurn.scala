package rescala.stm

import rescala.engine.ValuePersistency
import rescala.graph.Struct
import rescala.levelbased.{LevelBasedPropagation, LevelStruct}
import rescala.twoversion.Token

import scala.concurrent.stm.{InTxn, atomic}

class STMTurn extends LevelBasedPropagation[STMTurn] with LevelStruct {
  override type State[P, S <: Struct] = STMStructType[P, S]

  /** used to create state containers of each reactive */
  override protected def makeStructState[P](valuePersistency: ValuePersistency[P]): State[P, STMTurn] = {
    new STMStructType(valuePersistency.initialValue, valuePersistency.isTransient)
  }
  override def releasePhase(): Unit = ()
  // this is unsafe when used improperly
  def inTxn: InTxn = atomic(identity)
  override val token = Token(inTxn)
}


