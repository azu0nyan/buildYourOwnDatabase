package btree

import org.scalatest.funsuite.AnyFunSuite



class BTreeTest extends AnyFunSuite{
    test("Insert delete one"){
        val tree = new InMemoryBTree
        assert(tree.delete("kek") == false)
        assert(tree.getValue("kek").isEmpty)
        tree.insert("kek", "lol")
        assert(tree.getValue("kek").contains("lol"))
        assert(tree.delete("kek") == true)
        assert(tree.getValue("kek").isEmpty)
    }
}
