package btree

import org.scalatest.funsuite.AnyFunSuite


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

  def testFor(kvSeq: Seq[(String, String)], removeOrder: Option[Seq[String]] = None) =
    println(s"Testing for $kvSeq $removeOrder")
    val tree = new InMemoryBTree
    val keys = kvSeq.map(_._1)
    val values = kvSeq.map(_._2)
    for (k <- keys) assert(tree.delete(k) == false)
    for (k <- keys) assert(tree.getValue(k).isEmpty)

    for {
      (added, notAdded) <- kvSeq.indices.map(id => kvSeq.splitAt(id))
      (k, v) <- notAdded.headOption
    }
      println(s"insert $k $v")
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
      println(s"delete $toRemove")
      tree.delete(toRemove)
      assert(tree.getValue(toRemove).isEmpty)
      for (rk <- removed)
        assert(tree.getValue(rk).isEmpty)
      for (nrk <- notRemoved.tail)
        assert(tree.getValue(nrk).nonEmpty)
    }

  end testFor

  def testMany(n: Int) =
    val kvs = (1 to n).map(i => (s"KEY$i", s"VALUE$i"))
    for (s <- kvs.permutations; r <- kvs.map(_._1).permutations)
      testFor(s, Some(r))
  end testMany


  test("Insert delete one(2)") {
    testMany(1)
  }

  test("Insert delete two") {
    testMany(2)
  }

  test("Insert delete three") {
    testMany(3)
  }

  test("Insert delete many"){
    for(i <- 4 until 33){
      testMany(i)
    }
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


}
