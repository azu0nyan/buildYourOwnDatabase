package btree

import btree.BTree.{BNode, BTree, Pointer}

import scala.collection.mutable

class InMemoryBTree extends BTree {

  val m: mutable.Map[Pointer, BNode] = mutable.Map()
  var _root: Pointer = 0L
  var mId = 1L

  def setRoot(p: Pointer): Unit = _root = p
  // pointer (a nonzero page number)
  def root: Pointer = _root

  /** dereference a pointer */
  def get(p: Pointer): BNode = m(p)
  /** allocate new page */
  def alloc(bnode: BNode): Pointer = {    
    val ptr = mId
    mId += 1
    m += ptr -> bnode
    ptr
  }

  /** delete page */
  def del(p: Pointer): Unit =
    m -= p
}