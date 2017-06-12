package rescala.graph

import scala.annotation.implicitNotFound
import scala.util.Try

@implicitNotFound("${T} is not serializable")
trait Serializable[T] {
  def serialize(value: T): String
  def deserialize(value: String): Try[T]
}

object Serializable {
  implicit def everything[A]: Serializable[A] = null
}
