package btree

import btree.BTree.{BNode, Sizes}

import java.nio.ByteBuffer
import java.util
extension (node: BNode)
  def btype: Int = ByteBuffer.wrap(node.data).getInt(0)
  def nkeys: Int = ByteBuffer.wrap(node.data).getInt(4)
  def setHeader(btype: Int, nkeys: Int): Unit =
    ByteBuffer.wrap(node.data).putInt(0, btype).putInt(4, nkeys)

  def getPtr(id: Int):Long =
    //assert(id < node.nkeys) todo check
    val pos = Sizes.HEADER + Sizes.psize * id
    ByteBuffer.wrap(node.data).getLong(pos)
    
  def setPtr(id: Int, ptr: Long): Unit =
    //assert(id < node.nkeys) todo check
    val pos = Sizes.HEADER + Sizes.psize * id
    ByteBuffer.wrap(node.data).putLong(pos, ptr)

  /*
  -The offset is relative to the position of the first KV pair.
  -The offset of the first KV pair is always zero, so it is not stored in the list.
  -We store the offset to the end of the last KV pair in the offset list, which is used to
  determine the size of the node.
  */
  def offsetPos(id: Int): Int =
    //assert(1 <= id && id <= node.nkeys) todo check
    (Sizes.HEADER + Sizes.psize * nkeys + Sizes.offsetlen * (id - 1))

  def getOffset(id: Int): Int =
    if( id == 0) 0
    else ByteBuffer.wrap(node.data).getInt(offsetPos(id))

  def setOffset(id: Int, offset: Int): Unit =
    ByteBuffer.wrap(node.data).putInt(offsetPos(id), offset)

  def kvPos(id: Int): Int =
    //assert(id <= nkeys)
    (Sizes.HEADER + Sizes.psize * nkeys + Sizes.klen * nkeys + getOffset(id))

  def getKey(id: Int): Array[Byte] =
    //assert(id < nkeys)
    val pos = kvPos(id)
    val klen = ByteBuffer.wrap(node.data).getInt(pos)
    ByteBuffer.wrap(node.data).slice(pos + 4, klen).array()

  def getVal(id: Int): Array[Byte] =
    //assert(id < nkeys)
    val pos = kvPos(id)
    val klen = ByteBuffer.wrap(node.data).getInt(pos)
    val vlen = ByteBuffer.wrap(node.data).getInt(pos + Sizes.vlen)
    ByteBuffer.wrap(node.data).slice(pos + 4 + klen, vlen).array()

  def nbytes: Int = kvPos(nkeys)

  // returns the first kid node whose range intersects the key. (kid[i] <= key)
  def nodeLookupLE(key: Array[Byte]): Int =    
    (1 until nkeys).find(i => util.Arrays.compare(getKey(i), key) >= 0).getOrElse(0)









