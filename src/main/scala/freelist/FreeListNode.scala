package freelist

import btree.BTree
import btree.BTree.Pointer

import java.nio.ByteBuffer


/*
| type | size | total | next | pointers |
| 4B   | 4B   | 8B    | 8B   | size * 8B|
*/

object FreeListNode:
  
  object Consts{    
    val BNODE_FREE_LIST = 3
    val typeOffset = 0    
    val sizeOffset = typeOffset + 4    
    val totalOffset = sizeOffset + 4
    val nextOffset = totalOffset + 8
    val pointersOffset = nextOffset + 8
    val ptrLen = BTree.Sizes.pointerSize
    val FREE_LIST_CAP = (BTree.Const.BTREE_PAGE_SIZE - pointersOffset) / 8
  }
  
  
  opaque type FreeListNode = Array[Byte]

  object FreeListNode:
    def apply(arr: Array[Byte]): FreeListNode = arr

  extension (fln: FreeListNode)
    def bytes: Array[Byte] = fln
    
    def size: Int = ByteBuffer.wrap(fln).getInt(Consts.sizeOffset)
    def next: Pointer = ByteBuffer.wrap(fln).getLong(Consts.nextOffset)
    def total: Long = ByteBuffer.wrap(fln).getLong(Consts.totalOffset)
    def ptr(id: Int): Pointer = ByteBuffer.wrap(fln).getLong(Consts.pointersOffset + Consts.ptrLen * id)
    def setPtr(id: Int, ptr: Pointer): Unit = ByteBuffer.wrap(fln).putLong(Consts.pointersOffset + Consts.ptrLen * id, ptr)
    def setHeader(size: Int, next: Pointer): Unit =
      ByteBuffer.wrap(fln)
        .putInt(Consts.typeOffset, Consts.BNODE_FREE_LIST)
        .putInt(Consts.sizeOffset, size)
        .putLong(Consts.nextOffset, next)
    def setTotal(total: Long): Unit =
      ByteBuffer.wrap(fln)
        .putLong(Consts.totalOffset, total)
    
    
end FreeListNode
    
  
