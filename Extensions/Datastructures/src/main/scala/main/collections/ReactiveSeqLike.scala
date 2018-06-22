package main.collections

import rescala.default._

import scala.collection._
import scala.language.higherKinds

trait ReactiveSeqLike[A, ConcreteType[_]] extends ReactiveGenTraversableLike1[A, ConcreteType] {
	type InternalKind[B] <: SeqLike[B, InternalKind[B]]

	//Basic mutating functions
	val add: Signal[A] => Unit = liftMutating1((xs: InternalType, x: A) => (xs :+ x).asInstanceOf[InternalType]) _
	val append: Signal[InternalKind[A]] => Unit = liftMutating1((xs: InternalType, ys: InternalType) => (xs ++ ys).asInstanceOf[InternalType]) _

	val update: (Signal[Int], Signal[A]) => Unit = liftMutating2(_.updated(_: Int, _: A).asInstanceOf[InternalKind[A]])_

	def apply(i: Signal[Int]): Signal[A] = liftPure1(_.apply(_: Int))(i)

	val size: () => Signal[Int] = liftPure0(_.size) _
	val head: () => Signal[A] = liftPure0(_.head) _
	val last: () => Signal[A] = liftPure0(_.last) _
	val tail: () => Signal[InternalKind[A]] = liftPure0(_.tail) _

	//aliases
	def +=(elem: Signal[A]): Unit = {
	    add(elem)
	}
}
