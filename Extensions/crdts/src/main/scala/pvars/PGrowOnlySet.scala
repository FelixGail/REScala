package pvars

import rescala._
import statecrdts.sets.GSet

case class PGrowOnlySet[A](initial: GSet[A] = GSet[A](),
                           internalChanges: Evt[GSet[A]] = Evt[GSet[A]],
                           externalChanges: Evt[GSet[A]] = Evt[GSet[A]]) extends Publishable[GSet[A]] {

  def add(a: A): Unit = internalChanges(crdtSignal.now.add(a))

  def contains(a: A): Boolean = crdtSignal.now.contains(a)
}

object PGrowOnlySet {
  /**
    * Allows creation of DistributedSets by passing a set of initial values.
    */
  def apply[A](values: Set[A]): PGrowOnlySet[A] = {
    val init: GSet[A] = GSet().fromValue(values)
    new PGrowOnlySet[A](init)
  }
}