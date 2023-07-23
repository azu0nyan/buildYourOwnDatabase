package btree

import btree.BTree.Const.{BNODE_LEAF, BNODE_NODE}
import btree.BTree.{BNode, Pointer, Sizes}

import java.nio.ByteBuffer
import java.util
import scala.annotation.tailrec

/*
| type | nkeys | pointers    | offsets   | key-values
| 4B  | 4B     | nkeys * 8B | nkeys * 4B | ...
This is the format of the KV pair. Lengths followed by data.
| klen | vlen | key | val |
| 4B   | 4B   | ... | ... |
*/
extension (node: BNode)


  def prettyPrint: Unit = try {
    println("TYPE | KEYS |" + (0 until nkeys).map(i => f"p$i%-4d |").mkString("") +
      (0 until nkeys).map(i => f"k$i%-8d| v$i%-8d|").mkString(""))
    print(if (btype == BNODE_NODE) "NODE |" else "LEAF |")
    print(f" ${nkeys}%-4d |")
    print((0 until nkeys).map(i => f" ${getPtr(i)}%-4d |").mkString(""))
    for (i <- 0 until nkeys)
      print(f"${new String(getKey(i))}%-8s| ${new String(getVal(i))}%-8s |")
    println()
  } catch {
    case t: Throwable => println(s"MALFORMED NODE")
  }
  end prettyPrint

  def btype: Int = ByteBuffer.wrap(node.data).getInt(0)
  def nkeys: Int = ByteBuffer.wrap(node.data).getInt(4)
  def setHeader(btype: Int, nkeys: Int): Unit =
    ByteBuffer.wrap(node.data).putInt(0, btype).putInt(4, nkeys)

  def getPtr(id: Int): Pointer =
    //assert(id < node.nkeys) todo check
    val pos = Sizes.HEADER + Sizes.pointerSize * id
    ByteBuffer.wrap(node.data).getLong(pos)

  def setPtr(id: Int, ptr: Pointer): Unit =
    //assert(id < node.nkeys) todo check
    val pos = Sizes.HEADER + Sizes.pointerSize * id
    ByteBuffer.wrap(node.data).putLong(pos, ptr)

  /*
  -The offset is relative to the position of the first KV pair.
  -The offset of the first KV pair is always zero, so it is not stored in the list.
  -We store the offset to the end of the last KV pair in the offset list, which is used to
  determine the size of the node.
  */
  def offsetPos(id: Int): Int =
  //assert(1 <= id && id <= node.nkeys) todo check
    (Sizes.HEADER + Sizes.pointerSize * nkeys + Sizes.offsetlen * (id - 1)) // 8 + 2 * 8 + 0 = 24

  def getOffset(id: Int): Int =
    if (id == 0) 0
    else ByteBuffer.wrap(node.data).getInt(offsetPos(id))

  def setOffset(id: Int, offset: Int): Unit =
    ByteBuffer.wrap(node.data).putInt(offsetPos(id), offset)

  def kvPos(id: Int): Int =
  //assert(id <= nkeys)
    (Sizes.HEADER + Sizes.pointerSize * nkeys + Sizes.offsetlen * nkeys + getOffset(id)) //8 + 2 * 8 + 2 * 4 + 8 = 32 + 8

  def getKey(id: Int): Array[Byte] =
    //assert(id < nkeys)
    val pos = kvPos(id)    
    val klen = ByteBuffer.wrap(node.data).getInt(pos)
    val res = Array.ofDim[Byte](klen)
    ByteBuffer.wrap(node.data).slice(pos + Sizes.kvLen, klen).get(res)
    res
  end getKey


  def getVal(id: Int): Array[Byte] =
    //assert(id < nkeys)
    val pos = kvPos(id)
    val klen = ByteBuffer.wrap(node.data).getInt(pos)
    val vlen = ByteBuffer.wrap(node.data).getInt(pos + Sizes.vlen)
    val res = Array.ofDim[Byte](vlen)
    ByteBuffer.wrap(node.data).slice(pos + Sizes.kvLen + klen, vlen).get(res)
    res
  end getVal


  def nbytes: Int = kvPos(nkeys)

  /** returns the first kid node whose range intersects the key. (kid[i] <= key)
   * the first key is a copy from the parent node,
   * thus it's always less than or equal to the key.
   */
  def nodeLookupLE(key: Array[Byte]): Int =
    val max = nkeys
    @tailrec
    def kRec(id: Int): Int =
      if (id >= max)
        id - 1
      else
        val cmp = util.Arrays.compare(getKey(id), key)
        if (cmp < 0)
          kRec(id + 1)
        else if(cmp == 0)
          id
        else id - 1
    kRec(0) // can be 1
  end nodeLookupLE


end extension









