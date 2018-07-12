package rescala.crdts.statecrdts.sequences

import rescala.crdts.statecrdts.StateCRDT
import rescala.crdts.statecrdts.sets.RemovableSet

trait RemovableCRDTSequence[A] extends CRDTSequence[A] {
  def vertices: RemovableSet[ValueVertex[A]]

  def remove(v: ValueVertex[A])(implicit
                                stateCRDT: StateCRDT[valueType, selfType]): selfType =
    stateCRDT.fromPayload((vertices.remove(v), edges))
}
