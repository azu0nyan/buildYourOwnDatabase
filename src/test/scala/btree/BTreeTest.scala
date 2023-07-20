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
    val tree = new InMemoryBTree
    val keys = kvSeq.map(_._1)
    val values = kvSeq.map(_._2)
    for (k <- keys) assert(tree.delete(k) == false)
    for (k <- keys) assert(tree.getValue(k).isEmpty)
    for {
      (added, notAdded) <- kvSeq.indices.map(id => kvSeq.splitAt(id))
      (k, v) <- notAdded.headOption
    }
      tree.insert(k, v)
      assert(tree.getValue(k).contains(v))
      //test all keys
      for((ak, av) <- added)
        assert(tree.getValue(ak).contains(av))
      for((nk, nv) <- notAdded.tail)
        assert(tree.getValue(nk).isEmpty)
    end for

    val ro = removeOrder.getOrElse(kvSeq.map(_._1))
    for((removed, notRemoved) <- ro.indices.map(id => ro.splitAt(id));
        toRemove <- notRemoved.headOption
        ){
      tree.delete(toRemove)
      assert(tree.getValue(toRemove).isEmpty)
      for(rk <- removed)
        assert(tree.getValue(rk).isEmpty)
      for(nrk <- notRemoved.tail)
        assert(tree.getValue(nrk).nonEmpty)
    }

  end testFor


  test("Insert delete one(2)") {
    testFor(Seq(("KEY","VALUE")))
  }

  test("Insert delete two") {


  }


}
