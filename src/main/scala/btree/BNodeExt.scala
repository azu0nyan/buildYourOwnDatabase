package btree

import btree.BTree.{BNode, Sizes}

import java.nio.ByteBuffer
extension (node: BNode)
  def btype: Short = ByteBuffer.wrap(node.data).getShort(0)
  def nkeys: Short = ByteBuffer.wrap(node.data).getShort(2)
  def setHeader(btype: Short, nkeys: Short): Unit =
    ByteBuffer.wrap(node.data).putShort(0, btype).putShort(0, nkeys)

  def getPtr(id: Short):Long =
    //assert(id < node.nkeys) todo check
    val pos = Sizes.HEADER + Sizes.psize * id
    ByteBuffer.wrap(node.data).getShort(pos)
  def setPtr(id: Short, ptr: Long): Unit =
    //assert(id < node.nkeys) todo check
    val pos = Sizes.HEADER + Sizes.psize * id
    ByteBuffer.wrap(node.data).putLong(pos, ptr)

  /*
  -The offset is relative to the position of the first KV pair.
  -The offset of the first KV pair is always zero, so it is not stored in the list.
  -We store the offset to the end of the last KV pair in the offset list, which is used to
  determine the size of the node.
  */
  def offsetPos(id: Short): Short =
    //assert(1 <= id && id <= node.nkeys) todo check
    (Sizes.HEADER + Sizes.psize * nkeys + Sizes.offsetlen * (id - 1)).toShort

  def getOffset(id: Short): Short =
    if( id == 0) 0
    else ByteBuffer.wrap(node.data).getShort(offsetPos(id))

  def setOffset(id: Short, offset: Short): Unit =
    ByteBuffer.wrap(node.data).putShort(offsetPos(id).toInt, offset)

  def kvPos(id: Short): Short =
    //assert(id <= nkeys)
    (Sizes.HEADER + Sizes.psize * nkeys + Sizes.klen * nkeys + getOffset(id)).toShort

  def getKey(id: Short): Array[Byte] =
    //assert(id < nkeys)
    val pos = kvPos(id)
    val klen = ByteBuffer.wrap(node.data).getShort(pos)
    ByteBuffer.wrap(node.data).slice(pos + 4, klen).array()

  def getVal(id: Short): Array[Byte] =
    //assert(id < nkeys)
    val pos = kvPos(id)
    val klen = ByteBuffer.wrap(node.data).getShort(pos)
    val vlen = ByteBuffer.wrap(node.data).getShort(pos + Sizes.vlen)
    ByteBuffer.wrap(node.data).slice(pos + 4 + klen, vlen).array()

  def nbytes: Short = kvPos(nkeys)

  //no set key and set val









