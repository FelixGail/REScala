package rescala.crdts.statecrdts

trait StateCRDT[A, F] {
  /** the public state of the CRDT */
  def value(target: F): A

  /** Merge this instance with another CRDT instance and return the resulting CRDT. */
  def merge(left: F, right: F): F

  /** Allows the creation of new CRDTs by passing an initial value.
    *
    * @param value the value
    * @return new CRDT instance representing the value
    */
  def fromValue(value: A): F

  /** Allows the creation of new CRDTs by passing a payload.
    *
    * @param payload the payload
    * @return new CRDT instance with the given payload
    */
  //TODO: this can not be correct
  def fromPayload[P](payload: P): F
}

object StateCRDT {
  /**
    * Generates unique identifiers based on the current Hostname, IP address and a UUID based on the current system time.
    *
    * @return A new unique identifier (e.g. hostname/127.0.0.1::1274f9fe-cdf7-3f10-a7a4-33e8062d7435)
    */
  def genId: String = java.util.UUID.randomUUID().toString
}
