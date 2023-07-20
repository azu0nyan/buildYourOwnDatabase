package btree

import btree.BTree.BTree

extension (tree: BTree)
  def insert(key: Array[Byte], value: Array[Byte]): Unit =
    BTreeOps.insert(tree, key, value)  

  def insert(key: String, value: String): Unit =
    insert(key.getBytes, value.getBytes)

  def delete(key: Array[Byte]): Boolean =
    BTreeOps.delete(tree, key)
  
  def delete(key: String): Boolean =
    delete(key.getBytes)

  def getValue(key: Array[Byte]): Option[Array[Byte]] =
    BTreeOps.getValue(tree, key)

  def getValue(key: String): Option[String] =
    getValue(key.getBytes).map(_.toString)  


