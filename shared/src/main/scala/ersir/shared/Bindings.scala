package ersir.shared

import io.circe.generic.auto._
import loci.registry.Binding
import loci.serializer.circe._
import rescala.crdts.distributables.DistributedSignal._


object Bindings {
  val crdtDescriptions = Binding[rescala.crdts.distributables.PGrowOnlyLog[String]]("crdtDescriptions")
}
