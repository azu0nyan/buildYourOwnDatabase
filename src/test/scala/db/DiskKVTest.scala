package db

import btree.InMemoryBTree
import org.scalatest.funsuite.AnyFunSuite

import java.io.File


extension (kv: KV)
  def insert(k: String, v: String) : Unit=
    kv.set(k.getBytes, v.getBytes)
  def delete(k: String): Boolean =
    kv.delete(k.getBytes)
  def get(k: String): String =
    kv.get(k.getBytes).map(bs => new String(bs)).getOrElse("")

class DiskKVTest extends AnyFunSuite {
  def testFor(kvSeq: Seq[(String, String)], removeOrder: Option[Seq[String]] = None, print: Boolean = false) =
    if (print) println(s"Testing for $kvSeq $removeOrder")
    new File("testfile").delete()
    val kv = new DiskKV("testfile")
    val keys = kvSeq.map(_._1)
    val values = kvSeq.map(_._2)
    for (k <- keys) assert(kv.delete(k) == false)
    for (k <- keys) assert(kv.get(k).isEmpty)

    for {
      (added, notAdded) <- kvSeq.indices.map(id => kvSeq.splitAt(id))
      (k, v) <- notAdded.headOption
    }
      if (print) println(s"kv.insert(\"$k\", \"$v\")")
      kv.insert(k, v)
      assert(kv.get(k).contains(v))
      //test all keys
      for ((ak, av) <- added)
        assert(kv.get(ak).contains(av))
      for ((nk, nv) <- notAdded.tail)
        assert(kv.get(nk).isEmpty)
    end for

    val ro = removeOrder.getOrElse(kvSeq.map(_._1))
    for ((removed, notRemoved) <- ro.indices.map(id => ro.splitAt(id));
         toRemove <- notRemoved.headOption
         ) {
      if (print) println(s"kv.delete(\"$toRemove\")")
      kv.delete(toRemove)
      assert(kv.get(toRemove).isEmpty)
      for (rk <- removed)
        assert(kv.get(rk).isEmpty)
      for (nrk <- notRemoved.tail)
        if (print) println(nrk)
        assert(kv.get(nrk).nonEmpty)
    }

  end testFor


  def testManyPermutated(n: Int) =
    val kvs = (1 to n).map(i => (s"KEY$i", s"VALUE$i"))
    for (s <- kvs.permutations; r <- kvs.map(_._1).permutations)
      testFor(s, Some(r))
  end testManyPermutated


 test("Insert delete many 1 ") {
    val kvs = (1 to 16).map(i => (s"KEY$i", s"VALUE$i"))
    testFor(kvs)
  }

  test("Insert delete many 2 ") {
    val kvs = (1 to 128).map(i => (s"KEY$i", s"VALUE$i"))
    testFor(kvs)
  }
  test("Insert delete many 3 ") {
    val kvs = (1 to 256).map(i => (s"KEY$i", s"VALUE$i"))
    testFor(kvs)
  }

  test("Insert delete many 4") {
    val kvs = (1 to 1024).map(i => (s"KEY$i", s"VALUE$i"))
    testFor(kvs)
  }




}
