package rescala.pipelining

import rescala.graph.Spores.TraitStructP
import rescala.graph.{Buffer, Pulse, Spores}
import rescala.propagation.Turn

object PipelineSpores extends Spores {
  override type Struct[R] = PipelineStructP[_, R]

  override def bud[P, R](initialValue: Pulse[P], transient: Boolean, initialIncoming: Set[R]): StructP[P, R] = new PipelineStructP[P, R](initialValue, transient, initialIncoming)

  class PipelineStructP[P, R](initialValue: Pulse[P], transient: Boolean, initialIncoming: Set[R]) extends TraitStructP[P, R] {

    val pipeline: Pipeline = new Pipeline()

    private val _incoming: BlockingPipelineBuffer[Set[R]] = pipeline.createBlockingBuffer(initialIncoming, Buffer.commitAsIs)
    override def incoming(implicit turn: Turn[_]): Set[R] = _incoming.get
    def incomingForceGet(implicit turn: Turn[_]): Set[R] = _incoming.forceGet
    override def updateIncoming(reactives: Set[R])(implicit turn: Turn[_]): Unit = _incoming.set(reactives.toSet)


    private val lvl: NonblockingPipelineBuffer[Int] = pipeline.createNonblockingBuffer(0, math.max)
    override def level(implicit turn: Turn[_]): Int = lvl.get
    override def updateLevel(i: Int)(implicit turn: Turn[_]): Int = lvl.transform(math.max(i, _))


    private val _outgoing: NonblockingPipelineBuffer[Set[R]] = pipeline.createNonblockingBuffer(Set.empty, Buffer.commitAsIs)
    override def outgoing(implicit turn: Turn[_]): Set[R] = _outgoing.get
    override def discover(reactive: R)(implicit turn: Turn[_]): Unit = _outgoing.transform(_ + reactive)
    override def drop(reactive: R)(implicit turn: Turn[_]): Unit = _outgoing.transform(_ - reactive)


    override val pulses: Buffer[Pulse[P]] = pipeline.createBlockingBuffer(initialValue, if (transient) Buffer.transactionLocal else Buffer.keepPulse)
  }
}