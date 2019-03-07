package rescala.lattices

/** Well, its technically a semilattice, but thats just more to type.
  * Assumes */
trait Lattice[A] {
  /** Associative, commutative, idempotent. **/
  def merge(left: A, right: A): A
}

