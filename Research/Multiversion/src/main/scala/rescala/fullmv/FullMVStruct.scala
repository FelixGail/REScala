package rescala.fullmv

import rescala.core.{ReSource, Reactive, Struct}

trait FullMVStruct extends Struct {
  override type State[P, S <: Struct] = NodeVersionHistory[P, FullMVTurn, ReSource[FullMVStruct], Reactive[FullMVStruct]]
}
