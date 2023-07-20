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
    ByteBuffer.wrap(newNode.data).putInt(pos + Sizes.klen, value.length)
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
    val knode = tree.get(kptr)
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

  def nodeReplace2Kid(newNode: BNode, old: BNode, id: Int, ptr: Pointer, key: Array[Byte]): Unit =
    newNode.setHeader(BNODE_NODE, old.nkeys - 1)
    nodeAppendRange(newNode, old, 0, 0, id)
    nodeAppendKV(newNode, id, ptr, key, Array.ofDim[Byte](0))
    nodeAppendRange(newNode, old, id + 1, id + 2, old.nkeys - (id + 2))
  end nodeReplace2Kid



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

  def shouldMerge(tree: BTree, node: BNode, id: Int, updated: BNode): MergeDir =
    if (updated.nbytes > BTREE_PAGE_SIZE / 4)
      MergeDir.NONE
    else if (id > 0 && {
      val sibling = tree.get(node.getPtr(id - 1))
      val merged = sibling.nbytes + updated.nbytes - Sizes.HEADER
      merged <= BTREE_PAGE_SIZE
    })
      MergeDir.LEFT(tree.get(node.getPtr(id - 1)))
    else if (id + 1 < node.nkeys && {
      val sibling = tree.get(node.getPtr(id + 1))
      val merged = sibling.nbytes + updated.nbytes - Sizes.HEADER
      merged <= BTREE_PAGE_SIZE
    })
      MergeDir.RIGHT(tree.get(node.getPtr(id + 1)))
    else
      MergeDir.NONE


  def nodeDelete(tree: BTree, node: BNode, id: Int, key: Array[Byte]): Option[BNode] =
    val kptr = node.getPtr(id)
    treeDelete(tree, tree.get(kptr), key) match
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
            nodeReplaceKidN(tree, newNode, node, id, Seq(updated))
        Some(newNode)
      case None => None
  end nodeDelete

  def getValue(tree: BTree, node: BNode, key: Array[Byte]): Option[Array[Byte]] =
    val id = node.nodeLookupLE(key)
    node.btype match
      case `BNODE_LEAF` =>
        println("here"+ id + " "  + node.getKey(id).mkString(",") + " " + key.mkString(","))
        if (!util.Arrays.equals(key, node.getKey(id))) None
        else Some(node.getVal(id))
      case `BNODE_NODE` =>
        getValue(tree, node, key)
      case _ =>
        throw new RuntimeException(s"Bad node type ${node.btype}")
  end getValue

  def getValue(tree: BTree, key: Array[Byte]): Option[Array[Byte]] =
    assert(key.length != 0)
    assert(key.length <= BTREE_MAX_KEY_SIZE)
    if(tree.root == 0) None
    else getValue(tree, tree.get(tree.root), key)
  end getValue


  /**
   * The height of the tree will be reduced by one when:
   * 1. The root node is not a leaf.
   * 2. The root node has only one child.
   */
  def delete(tree: BTree, key: Array[Byte]): Boolean =
    assert(key.length != 0)
    assert(key.length <= BTREE_MAX_KEY_SIZE)
    if (tree.root == 0) false
    else
      treeDelete(tree, tree.get(tree.root), key) match
        case Some(updated) =>
          tree.del(tree.root)
          if (updated.btype == BNODE_NODE && updated.nkeys == 1)
            tree.setRoot(updated.getPtr(0))
          else
            tree.setRoot(tree.alloc(updated))
          true
        case None =>
          false
  end delete

  def insert(tree: BTree, key: Array[Byte], value: Array[Byte]): Unit =
    assert(key.length != 0)
    assert(key.length <= BTREE_MAX_KEY_SIZE)
    assert(value.length <= BTREE_MAX_VAL_SIZE)
    if (tree.root == 0)
      //create first node
      val root = new BNode(Array.ofDim[Byte](BTREE_PAGE_SIZE))
      root.setHeader(BNODE_LEAF, 2)
      // a dummy key, this makes the tree cover the whole key space.
      // thus a lookup can always find a containing node.
      nodeAppendKV(root, 0, 0, Array.ofDim[Byte](0), Array.ofDim[Byte](0))
      nodeAppendKV(root, 1, 0, key, value)
      tree.setRoot(tree.alloc(root))
    else
      val node = tree.get(tree.root)
      tree.del(tree.root)
      val nodeIns = treeInsert(tree, node, key, value)
      val splited = nodeSplit3(nodeIns)
      if (splited.size > 1)
        // the root was split, add a new level.
        val root = new BNode(Array.ofDim[Byte](BTREE_PAGE_SIZE))
        root.setHeader(BNODE_NODE, splited.size)
        for ((knode, id) <- splited.zipWithIndex)
          val kptr = tree.alloc(knode)
          val kkey = knode.getKey(0)
          nodeAppendKV(root, id, kptr, kkey, Array.ofDim[Byte](0))
        tree.setRoot(tree.alloc(root))
      else
        tree.setRoot(tree.alloc(splited.head))
  end insert


end BTreeOps

