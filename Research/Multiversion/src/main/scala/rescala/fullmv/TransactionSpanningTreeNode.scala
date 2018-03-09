package rescala.fullmv
import java.util

trait TransactionSpanningTreeNode[T] {
  val txn: T
  def childCount(): Int
  def iterator(): java.util.Iterator[_ <: TransactionSpanningTreeNode[T]]
}

class MutableTransactionSpanningTreeNode[T](val txn: T) extends TransactionSpanningTreeNode[T] {
  @volatile var children: Array[MutableTransactionSpanningTreeNode[T]] = new Array(6)
  @volatile var size: Int = 0

  // callers must execute mutually exclusive.
  // updates size after the update is complete; if the array reference is replaced, the original are kept intact.
  // readers can thus safely read the set of children concurrently by first reading size, then reading the array reference, and then iterating.
  // iterator() is thread-safe following this pattern in that first calling hasNext() compares against size, and calling next() afterwards is then safe.
  // concurrent writes may or may not be visible, clients must manually implement according synchronization if required.
  def addChild(child: MutableTransactionSpanningTreeNode[T]): Unit = {
    if(children.length == size) {
      val newChildren = new Array[MutableTransactionSpanningTreeNode[T]](children.length + (children.length >> 1))
      System.arraycopy(children, 0, newChildren, 0, size)
      children = newChildren
    }
    children(size) = child
    size += 1
  }

  override def childCount(): Int = size
  override def iterator(): util.Iterator[MutableTransactionSpanningTreeNode[T]] = new util.Iterator[MutableTransactionSpanningTreeNode[T]] {
    private var idx = 0
    override def next(): MutableTransactionSpanningTreeNode[T] = {
      val r = children(idx)
      idx += 1
      r
    }
    override def hasNext: Boolean = idx < size
  }
}
