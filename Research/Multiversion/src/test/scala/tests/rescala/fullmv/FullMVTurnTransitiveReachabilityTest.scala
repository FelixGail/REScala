package tests.rescala.fullmv

import org.scalatest.FunSuite
import rescala.fullmv.{FullMVEngine, FullMVTurn}

import scala.util.Random

class FullMVTurnTransitiveReachabilityTest extends FunSuite {
  val engine = new FullMVEngine("reachability test")

  case class Disagreement[T](from: T, to: T, closure: Boolean, addEdgeSearchPath: Boolean)

  private def findDisagreements[T](nodes: Set[T], trees: Map[T, FullMVTurn], transitiveClosure: Map[T, Set[T]]) = {
    for (from <- nodes; to <- nodes if transitiveClosure(from)(to) != trees(from).isTransitivePredecessor(trees(to)))
      yield Disagreement(from, to, transitiveClosure(from)(to), trees(from).isTransitivePredecessor(trees(to)))
  }

  private def computeTransitiveClosure[T](nodes: Set[T], edges: Map[T, Set[T]]) = {
    var transitiveClosure = nodes.map(node => node -> (edges.getOrElse(node, Set.empty) + node)).toMap
    for (via <- edges.keySet) {
      for (from <- edges.keySet if transitiveClosure(from)(via); to <- transitiveClosure(via)) {
        transitiveClosure += from -> (transitiveClosure(from) + to)
      }
      transitiveClosure += via -> (transitiveClosure(via) + via)
    }
    transitiveClosure
  }

  private def makeTreesUnderSingleLockedLock(nodes: Set[Int]) = {
    val trees: Map[Int, FullMVTurn] = nodes.map(e => (e, engine.newTurn())).toMap
    // put all transactions under a common locked lock, so that all locking assertions hold
    trees.values.foreach(_.beginExecuting())
    val lock = engine.lock
    lock.lock()
    (trees, lock)
  }

  test("Digraph Reachability is correct for paper graph") {
    // G.F. Italiano 1986. "Amortized Efficiency Of A Path Retrieval Data Structure"
    val edges = Map[Int, Set[Int]](
      1 -> Set(2, 4, 5, 7),
      2 -> Set(7, 8, 9, 12),
      3 -> Set(9),
      4 -> Set(3, 7),
      5 -> Set(4, 6),
      6 -> Set(3, 10),
      7 -> Set(9),
      8 -> Set(1, 5, 6, 11),
      9 -> Set(10),
      10 -> Set(),
      11 -> Set(),
      12 -> Set(11)
    )

    val nodes = edges.keySet

    val (trees, locked) = makeTreesUnderSingleLockedLock(nodes)
    try {
      var addedEdges = Map[Int, Set[Int]]().withDefaultValue(Set())
      for ((from, tos) <- edges; to <- tos) {
        addedEdges = addEdgeIfPossibleAndVerify(nodes, trees, addedEdges, from, to)
      }
    } finally {
      locked.unlock()
    }
  }

  private def addEdgeIfPossibleAndVerify(nodes: Set[Int], trees: Map[Int, FullMVTurn], addedEdges: Map[Int, Set[Int]], from: Int, to: Int): Map[Int, Set[Int]] = {
    val fromTree = trees(from)
    val toTree = trees(to)
    val res = if (!fromTree.isTransitivePredecessor(toTree)) {
      fromTree.addPredecessor(toTree.selfNode)
      addedEdges + (from -> (addedEdges(from) + to))
    } else addedEdges

    val transitiveClosure = computeTransitiveClosure(nodes, res)
    assert(findDisagreements(nodes, trees, transitiveClosure) == Set.empty, "found disagreement after edges " + addedEdges)

    res
  }

  test("Random edges") {
    val SIZE = 31
    val random = new Random()
    val nodes = (0 until SIZE).toSet
    val (trees, lock) = makeTreesUnderSingleLockedLock(nodes)
    try {
      var addedEdges = Map[Int, Set[Int]]().withDefaultValue(Set())
      for (_ <- 0 until SIZE * SIZE) {
        val from, to = random.nextInt(SIZE)
        addedEdges = addEdgeIfPossibleAndVerify(nodes, trees, addedEdges, from, to)
      }
    } finally {
      lock.unlock()
    }
  }
}
