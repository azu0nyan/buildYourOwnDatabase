package db

import btree.{BTree, BTreeOps}
import btree.BTree.{BNode, BTree, Pointer}
import diskStorage.DiskStorage

import java.nio.ByteBuffer

class DiskKV(file: String) extends KV {

  val storage = new DiskStorage(file)

  val btree = new BTree {
    var _root = storage.getUserData.position(0).getLong
    def setRoot(p: Pointer): Unit = _root = p
    def root: Pointer = _root
    def get(p: Pointer): BNode = BNode(storage.pageGet(p))
    def alloc(bnode: BNode): Pointer = storage.pageNew(bnode.data)
    def del(p: Pointer): Unit = storage.pageDel(p)
  }

  def masterPageData(): Array[Byte] =
    val a = Array.ofDim[Byte](8)
    ByteBuffer.wrap(a).putLong(btree.root)
    a
  end masterPageData

  override def get(key: Array[Byte]): Option[Array[Byte]] =
    BTreeOps.getValue(btree, key)

  override def set(key: Array[Byte], value: Array[Byte]): Unit =
    BTreeOps.insert(btree, key, value)
    storage.flushPages()
    storage.writeMasterPage(masterPageData())

  override def delete(key: Array[Byte]): Boolean =
    val res = BTreeOps.delete(btree, key)
    storage.flushPages()
    storage.writeMasterPage(masterPageData())
    res
}
