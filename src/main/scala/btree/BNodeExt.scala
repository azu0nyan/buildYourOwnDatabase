package btree

import btree.BTree.{BNode, Sizes}

import java.nio.ByteBuffer
extension (bnode: BNode)
  def btype: Short = ByteBuffer.wrap(bnode.data).getShort(0)
  def nkeys: Short = ByteBuffer.wrap(bnode.data).getShort(2)
  def setHeader(btype: Short, nkeys: Short): Unit =
    ByteBuffer.wrap(bnode.data).putShort(0, btype).putShort(0, nkeys)
  def getPtr(id: Short):Long =
    val pos = Sizes.HEADER + Sizes.



