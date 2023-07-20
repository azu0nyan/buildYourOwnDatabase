package btree

import btree.BTree.Const.*
import btree.BTree.{BNode, BTree, Pointer, Sizes}

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
    nodeAppendKV(newNode, id, 0, key, value)
    nodeAppendRange(newNode, old, id + 1, id, old.nkeys - id)
  end leafInsert

  def leafUpdate(newNode: BNode, old: BNode, id: Int, key: Array[Byte], value: Array[Byte]): Unit =
    newNode.setHeader(BNODE_LEAF, old.nkeys)
    nodeAppendRange(newNode, old, 0, 0, id - 1)
    nodeAppendKV(newNode, id, 0, key, value)
    nodeAppendRange(newNode, old, id + 1, id + 1, old.nkeys - id - 1)
  end leafUpdate

  def nodeAppendRange(newNode: BNode, old: BNode, dstNew: Int, srcOld: Int, n: Int): Unit =
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

  def nodeAppendKV(newNode: BNode, id: Int, ptr: Pointer, key: Array[Byte], value: Array[Byte]): Unit =
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
        if (util.Arrays.equals(key, node.getKey(id)))
        // found the key, update it.
          leafUpdate(newNode, node, id, key, value)
        else
        //// insert it after the position.
          leafInsert(newNode, node, id, key, value)
      case `BNODE_NODE` =>
        // internal node, insert it to a kid node.
        nodeInsert(tree, newNode, node, id, key, value)
      case _ =>
        throw new RuntimeException(s"Bad node type ${node.btype}")

    newNode

  end treeInsert

  // part of the treeInsert(): KV insertion to an internal node
  def nodeInsert(tree: BTree, newNode: BNode, node: BNode, id: Int, key: Array[Byte], value: Array[Byte]): Unit =
    // get and deallocate the kid node
    val kptr = node.getPtr(id)
    val knode = tree.get(kptr).get
    tree.del(kptr)
    // recursive insertion to the kid node
    val knodeIns = treeInsert(tree, knode, key, value)
    val splited = nodeSplit3(knode)
    // update the kid links
    nodeReplaceKidN(tree, newNode, node, id, splited)
  end nodeInsert

  /**
   * // split a bigger-than-allowed node into two.
   * // the second node always fits on a page.
   */
  def nodeSplit2(left: BNode, right: BNode, old: BNode): Unit = {
    val kvOverhead = Sizes.psize + Sizes.kvLen

    val (rightSize, minRightId) = ((old.nkeys - 1) to 0).foldRight((Sizes.HEADER, old.nkeys - 1)) {
      case (id, (currentSize, minId)) =>
        val kvLen = old.getKey(id).length + old.getVal(id).length //todo use offsets to calculate or mb do NodeAppend while traversing
        if (currentSize + kvOverhead + kvLen <= BTREE_PAGE_SIZE)
          (currentSize + kvOverhead + kvLen, id)
        else
          (currentSize, minId)
    }

    val rightNKeys = old.nkeys - minRightId
    left.setHeader(old.btype, minRightId) //todo check header
    right.setHeader(old.btype, rightNKeys)

    for (i <- 0 until minRightId)
      nodeAppendKV(left, i, old.getPtr(i), old.getKey(i), old.getVal(i))

    for (i <- 0 until rightNKeys)
      nodeAppendKV(left, i, old.getPtr(i + minRightId), old.getKey(i + minRightId), old.getVal(i + minRightId))

  }

  def nodeSplit3(old: BNode): Seq[BNode] =
    if (old.nbytes <= BTREE_PAGE_SIZE)
      Seq(new BNode(old.data.take(BTREE_PAGE_SIZE)))
    else
      val left = new BNode(Array.ofDim[Byte](2 * BTREE_PAGE_SIZE))
      val right = new BNode(Array.ofDim[Byte](BTREE_PAGE_SIZE))
      nodeSplit2(left, right, old)
      if (left.nbytes <= BTREE_PAGE_SIZE)
        Seq(new BNode(left.data.take(BTREE_PAGE_SIZE)), right)
      else
        val leftleft = new BNode(Array.ofDim[Byte](BTREE_PAGE_SIZE))
        val middle = new BNode(Array.ofDim[Byte](BTREE_PAGE_SIZE))
        nodeSplit2(leftleft, middle, left)
        //assert(leftleft.nbytes() <= BTREE_PAGE_SIZE)
        Seq(leftleft, middle, right)
  end nodeSplit3

  def nodeReplaceKidN(tree: BTree, newNode: BNode, old: BNode, id: Int, kids: Seq[BNode]): Unit =
    newNode.setHeader(BNODE_NODE, old.nkeys + kids.size - 1)
    nodeAppendRange(newNode, old, 0, 0, id)
    for ((node, i) <- kids.zipWithIndex)
      nodeAppendKV(newNode, id + i, tree.alloc(node), node.getKey(0), Array.ofDim[Byte](0))
    nodeAppendRange(newNode, old, id + kids.size, id + 1, old.nkeys - (id + 1))
  end nodeReplaceKidN


  /** remove a key from a leaf node */
  def leafDelete(newNode: BNode, old: BNode, id: Int): Unit =
    newNode.setHeader(BNODE_LEAF, old.nkeys - 1)
    nodeAppendRange(newNode, old, 0, 0, id)
    nodeAppendRange(newNode, old, id, id + 1, old.nkeys - (id + 1))
  end leafDelete


  /** delete a key from the tree */
  def treeDelete(tree: BTree, node: BNode, key: Array[Byte]): Option[BNode] =
    val id = node.nodeLookupLE(key)
    node.btype match
      case `BNODE_LEAF` =>
        if (!util.Arrays.equals(key, node.getKey(id))) None
        else
          val newNode = new BNode(Array.ofDim[Byte](BTREE_PAGE_SIZE))
          leafDelete(newNode, node, id)
          Some(newNode)
      case `BNODE_NODE` =>
        nodeDelete(tree, node, id, key)
      case _ =>
        throw new RuntimeException(s"Bad node type ${node.btype}")
  end treeDelete

  enum MergeDir:
    case NONE
    case LEFT(sibling: BNode)
    case RIGHT(sibling: BNode)

  def nodeMerge(newNode: BNode, left: BNode, right: BNode): Unit =
    newNode.setHeader(left.btype, left.nkeys + right.nkeys)
    nodeAppendRange(newNode, left, 0, 0, left.nkeys)
    nodeAppendRange(newNode, left, left.nkeys, 0, right.nkeys)
  end nodeMerge

  def shouldMerge(tree: BTree, node: BNode, id: Int, updated: BNode): MergeDir = ???


  def nodeReplace2Kid(newNode: BNode, node: BNode, id: Int, ptr: Pointer, key: Array[Byte]) = ???
  def nodeReplaceKidN(tree: BTree, newNode: BNode, node: BNode, id: Int, updated: BNode) = ???

  def nodeDelete(tree: BTree, node: BNode, id: Int, key: Array[Byte]): Option[BNode] =
    val kptr = node.getPtr(id)
    treeDelete(tree, tree.get(kptr).get, key) match
      case Some(updated) =>
        tree.del(kptr)
        val newNode = new BNode(Array.ofDim[Byte](BTREE_PAGE_SIZE))

        shouldMerge(tree, node, id, updated) match
          case MergeDir.LEFT(sibling) =>
            val merged = new BNode(Array.ofDim[Byte](BTREE_PAGE_SIZE))
            nodeMerge(merged, sibling, updated)
            tree.del(node.getPtr(id - 1))
            nodeReplace2Kid(newNode, node, id - 1, tree.alloc(merged), merged.getKey(0))
          case MergeDir.RIGHT(sibling) =>
            val merged = new BNode(Array.ofDim[Byte](BTREE_PAGE_SIZE))
            nodeMerge(merged, updated, sibling)
            tree.del(node.getPtr(id - 1))
            nodeReplace2Kid(newNode, node, id - 1, tree.alloc(merged), merged.getKey(0))
          case MergeDir.NONE =>
            //assert(updated.nkeys() > 0)
            nodeReplaceKidN(tree, newNode, node, id, updated)
        Some(newNode)
      case None => None
  end nodeDelete


end BTreeOps

