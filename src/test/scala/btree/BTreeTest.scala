package btree

import btree.BTree.BTree
import org.scalatest.funsuite.AnyFunSuite

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
    getValue(key.getBytes).map(bs => new String(bs))
end extension


class BTreeTest extends AnyFunSuite {
  test("Insert delete one") {
    val tree = new InMemoryBTree
    assert(tree.delete("kek") == false)
    assert(tree.getValue("kek").isEmpty)
    tree.insert("kek", "lol")
    assert(tree.getValue("kek").contains("lol"))
    assert(tree.delete("kek") == true)
    assert(tree.getValue("kek").isEmpty)
  }

  def testFor(kvSeq: Seq[(String, String)], removeOrder: Option[Seq[String]] = None, print: Boolean = false) =
    if (print) println(s"Testing for $kvSeq $removeOrder")
    val tree = new InMemoryBTree
    val keys = kvSeq.map(_._1)
    val values = kvSeq.map(_._2)
    for (k <- keys) assert(tree.delete(k) == false)
    for (k <- keys) assert(tree.getValue(k).isEmpty)

    for {
      (added, notAdded) <- kvSeq.indices.map(id => kvSeq.splitAt(id))
      (k, v) <- notAdded.headOption
    }
      if (print) println(s"tree.insert(\"$k\", \"$v\")")
      tree.insert(k, v)
      assert(tree.getValue(k).contains(v))
      //test all keys
      for ((ak, av) <- added)
        assert(tree.getValue(ak).contains(av))
      for ((nk, nv) <- notAdded.tail)
        assert(tree.getValue(nk).isEmpty)
    end for

    val ro = removeOrder.getOrElse(kvSeq.map(_._1))
    for ((removed, notRemoved) <- ro.indices.map(id => ro.splitAt(id));
         toRemove <- notRemoved.headOption
         ) {
      if (print) println(s"tree.delete(\"$toRemove\")")
      tree.delete(toRemove)
      assert(tree.getValue(toRemove).isEmpty)
      for (rk <- removed)
        assert(tree.getValue(rk).isEmpty)
      for (nrk <- notRemoved.tail)
        if (print) println(nrk)
        assert(tree.getValue(nrk).nonEmpty)
    }

  end testFor

  def testManyPermutated(n: Int) =
    val kvs = (1 to n).map(i => (s"KEY$i", s"VALUE$i"))
    for (s <- kvs.permutations; r <- kvs.map(_._1).permutations)
      testFor(s, Some(r))
  end testManyPermutated


  test("Insert delete one(2)") {
    testManyPermutated(1)
  }

  test("Insert delete two") {
    testManyPermutated(2)
  }

  test("Insert delete three") {
    testManyPermutated(3)
  }

  test("Insert delete many permutated") {
    for (i <- 4 until 6) {
      testManyPermutated(i)
    }
  }

  test("Insert delete many") {
    val kvs = (1 to 1024).map(i => (s"KEY$i", s"VALUE$i"))
    testFor(kvs)
  }

  test("bug #1") {
    val tree = new InMemoryBTree
    tree.insert("KEY1", "VALUE1")
    tree.insert("KEY2", "VALUE2")
    assert(tree.getValue("KEY1").contains("VALUE1"))
  }

  test("bug #2") {
    val tree = new InMemoryBTree
    tree.insert("KEY1", "VALUE1")
    tree.insert("KEY2", "VALUE2")
    tree.insert("KEY3", "VALUE3")
    assert(tree.getValue("KEY3").contains("VALUE3"))
  }

  test("bug #3") {
    val tree = new InMemoryBTree
    tree.insert("KEY1", "VALUE1")
    tree.insert("KEY2", "VALUE2")
    tree.insert("KEY3", "VALUE3")
    tree.insert("KEY4", "VALUE4")
    assert(tree.getValue("KEY4").contains("VALUE4"))
  }

  test("bug #4") {
    val tree = new InMemoryBTree
    tree.insert("KEY1", "VALUE1")
    tree.insert("KEY2", "VALUE2")
    tree.insert("KEY5", "VALUE5")
    tree.insert("KEY4", "VALUE4")
    tree.insert("KEY3", "VALUE3")
    tree.delete("KEY1")
    tree.delete("KEY2")
    tree.delete("KEY3")
    tree.delete("KEY5")
    assert(tree.getValue("KEY4").contains("VALUE4"))
  }

  test("bug #5") {
    val tree = new InMemoryBTree
    tree.insert("KEY1", "VALUE1")
    tree.insert("KEY2", "VALUE2")
    tree.insert("KEY5", "VALUE5")
    tree.insert("KEY4", "VALUE4")
    tree.insert("KEY3", "VALUE3")
    tree.delete("KEY5")
    tree.delete("KEY1")
    tree.delete("KEY2")
    assert(tree.getValue("KEY3").contains("VALUE3"))
  }
}
