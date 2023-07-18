package btree

object BTree {

  /**Byte sizes of node*/
  object Sizes {
    def `type`: Short = 2
    def nodeType = `type`
    def nkeys: Short = 2
    def psize: Short = 8
    /** used keys instead of nkeys */
    def pointers(keys: Short): Short = (psize * keys).toShort
    def offsets(keys: Short): Short = (keys * 2).toShort

    val HEADER = Sizes.`type` + Sizes.nkeys

    def klen: Short = 2.toShort
    def vlen: Short = 2.toShort
    def kvLen: Short = (klen + vlen).toShort 
    def offsetlen: Short = 2.toShort
  }

  object Const {
    val BNODE_NODE = 1
    val BNODE_LEAF = 2


    val BTREE_PAGE_SIZE = 4096
    val BTREE_MAX_KEY_SIZE = 1000
    val BTREE_MAX_VAL_SIZE = 3000
    /**following structure of a packet*/
    val node1max = Sizes.HEADER + Sizes.pointers(1) + Sizes.offsets(1) + Sizes.klen + Sizes.vlen + 4 + BTREE_MAX_KEY_SIZE + BTREE_MAX_VAL_SIZE
    assert(node1max < BTREE_PAGE_SIZE)
  }

  class BNode(val data: Array[Byte])

  type Pointer = Long
  trait BTree {
    // pointer (a nonzero page number)
    def root: Pointer
    // callbacks for managing on-disk pages
    /** dereference a pointer */
    def get(p: Pointer): Option[BNode]
    /**allocate new page */
    def alloc(bnode: BNode): Pointer
    /**delete page*/
    def del(p: Pointer): Unit
  }




}


