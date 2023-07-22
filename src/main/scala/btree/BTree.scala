package btree

object BTree {

  /**Byte sizes of node*/
  object Sizes {

    def `type` = 4
    def nodeType = `type`

    def nkeys = 4
    def psize = 8
    /** used keys instead of nkeys */
    def pointers(keys:Int) = (psize * keys)
    def offsets(keys:Int) = (keys * 2)

    val HEADER = Sizes.`type` + Sizes.nkeys

    def klen = 4
    def vlen = 4
    def kvLen = (klen + vlen) 
    def offsetlen = 4
  }


  object Const {
    val BNODE_NODE = 1
    val BNODE_LEAF = 2


    val BTREE_PAGE_SIZE = 128
    val BTREE_MAX_KEY_SIZE = 32
    val BTREE_MAX_VAL_SIZE = 32
    val BTREE_MERGE_FACTOR = 0.25
    /**following structure of a packet*/
    val node1max = Sizes.HEADER + Sizes.pointers(1) + Sizes.offsets(1) + Sizes.klen + Sizes.vlen + BTREE_MAX_KEY_SIZE + BTREE_MAX_VAL_SIZE
    assert(node1max < BTREE_PAGE_SIZE)
  }

  class BNode(val data: Array[Byte])

  type Pointer = Long
  trait BTree {

    def setRoot(p: Pointer): Unit
    // pointer (a nonzero page number)
    def root: Pointer
    // callbacks for managing on-disk pages
    /** dereference a pointer */
    def get(p: Pointer): BNode
    /**allocate new page */
    def alloc(bnode: BNode): Pointer
    /**delete page*/
    def del(p: Pointer): Unit
  }





}


