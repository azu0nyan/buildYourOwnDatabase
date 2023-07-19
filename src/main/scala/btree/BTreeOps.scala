package btree

import btree.BTree.Const.*
import btree.BTree.{BNode, BTree, Sizes}

import java.nio.ByteBuffer
import java.util
import scala.annotation.tailrec

object BTreeOps:


  //  def leafInsert(id: Int, key: Array[Byte], value: Array[Byte]): BNode =
  //    val node = new BNode(BTREE_PAGE_SIZE)
  //    leafInsert(node, id, key, value)

  def leafInsert(newNode: BNode, old: BNode, id: Int, key: Array[Byte], value: Array[Byte]): Unit =
    newNode.setHeader(BNODE_LEAF, old.nkeys + 1)
    nodeAppendRange(newNode, old, 0, 0, id)
    nodeAppendKV(newNode,  id, 0, key, value)
    nodeAppendRange(newNode, old, id + 1, id, old.nkeys - id)
  end leafInsert

  def leafUpdate(newNode: BNode, old: BNode, id: Int, key: Array[Byte], value: Array[Byte]): Unit =
    newNode.setHeader(BNODE_LEAF, old.nkeys)
    nodeAppendRange(newNode, old, 0, 0, id - 1)
    nodeAppendKV(newNode, id, 0, key, value)
    nodeAppendRange(newNode, old, id + 1, id + 1, old.nkeys - id - 1)
  end leafUpdate

  def nodeAppendRange(newNode: BNode,old:BNode,  dstNew: Int, srcOld: Int, n: Int): Unit =
  //assert(srcOld + n <= old.nkeys())
  //assert(dstNew + n <= new.nkeys())
    if (n > 0)
      for (i <- 0 until n)
        newNode.setPtr(dstNew + i, old.getPtr(srcOld + i))

      val dstBegin = newNode.getOffset(dstNew)
      val srcBegin = old.getOffset(srcOld)
      for (i <- 1 to n) { //// NOTE: the range is [1, n]
        val offset = dstBegin + old.getOffset(srcOld + i) - srcBegin
        newNode.setOffset(dstNew + i, offset)
      }
      val begin = old.kvPos(srcOld)
      val end = old.kvPos(srcOld + n)
      old.data.slice(begin, end).copyToArray(newNode.data, newNode.kvPos(dstNew))
  end nodeAppendRange

  def nodeAppendKV(newNode: BNode,  id: Int, ptr: Int, key: Array[Byte], value: Array[Byte]): Unit =
    newNode.setPtr(id, ptr)

    val pos = newNode.kvPos(id)
    ByteBuffer.wrap(newNode.data).putInt(pos, key.length)
    ByteBuffer.wrap(newNode.data).putInt(pos + Sizes.klen, key.length)
    key.copyToArray(newNode.data, pos + Sizes.klen + Sizes.vlen)
    value.copyToArray(newNode.data, pos + Sizes.klen + Sizes.vlen + key.length)
    // the offset of the next key
    newNode.setOffset(id + 1, newNode.getOffset(id) + Sizes.klen + Sizes.vlen + key.length + value.length)
  end nodeAppendKV

  /**
   * insert a KV into a node, the result might be split into 2 nodes.
   * the caller is responsible for deallocating the input node
   * and splitting and allocating result nodes.
   */
  def treeInsert(tree: BTree, node: BNode, key: Array[Byte], value: Array[Byte]): BNode =
    // the result node.  it's allowed to be bigger than 1 page and will be split if so
    val newNode = new BNode(Array.ofDim[Byte](2 * BTREE_PAGE_SIZE))

    val id = node.nodeLookupLE(key)
    node.btype match
      case `BNODE_LEAF` =>
        //// leaf, node.getKey(idx) <= key
        if(util.Arrays.equals(key, node.getKey(id)))
          // found the key, update it.
             leafUpdate(newNode, node, id, key,    value )
        else
          //// insert it after the position.
          leafInsert(newNode, node, id, key, value)
      case `BNODE_NODE` =>
        // internal node, insert it to a kid node.
         nodeInsert(tree, newNode, node, id, key,   value)
      case _ =>
        throw  new RuntimeException(s"Bad node type ${node.btype}")

    newNode

  end treeInsert

  // part of the treeInsert(): KV insertion to an internal node
  def nodeInsert(tree: BTree, newNode: BNode, node: BNode, id: Int, key: Array[Byte], value: Array[Byte]): Unit =
    // get and deallocate the kid node
    val kptr = node.getPtr(id)
    val knode = tree.get(kptr)
    tree.del(kptr)
    // recursive insertion to the kid node
    val knodeIns = treeInsert(tree, knode, key,  value )
//    val (nsplit, slited) = nodeSplit3(knode)
    // update the kid links
//    nodeReplaceKidN(tree, new, node, idx, splited[: nsplit  ]... )
  end nodeInsert




end BTreeOps

