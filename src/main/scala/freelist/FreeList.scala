package freelist

import btree.BTree.Pointer
import freelist.FreeListNode.FreeListNode

import scala.annotation.tailrec

//trait FreeList {
//  def total():Lom
//  def get(topn: Int): Pointer
//  def update(popn: Int, freed: Seq[Pointer]): Unit
//}

trait FreeList {
  def head: Pointer
  def get(ptr: Pointer): FreeListNode
  def alloc(topn: FreeListNode): Pointer
  def use(ptr: Pointer, fln: FreeListNode): Unit

  def total: Long = get(head).total
}

extension (fl: FreeList)
  def get(topn: Int): Pointer =
    //  assert(0 <= topn && topn < fl.total())
    @tailrec
    def getRec(fln: FreeListNode, id: Int): Pointer =
      if (fln.size < id) fln.ptr(id)
      else getRec(fl.get(fln.next), id - fln.size)
    getRec(fl.get(fl.head), topn)
  end get

  def update(popn: Int, freed: Seq[Pointer]): Unit =
    assert(popn < fl.total())
    if(popn > 0 || freed.nonEmpty)
      val total = fl.total
      val freed = mutable.Buffer(fl.head)
    for()


  end update
