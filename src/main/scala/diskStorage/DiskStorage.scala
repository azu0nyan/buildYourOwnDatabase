package diskStorage

import btree.BTree.Pointer

import java.io.RandomAccessFile
import java.nio.MappedByteBuffer
import java.nio.channels.FileChannel
import java.util.concurrent.atomic.AtomicLong
import scala.collection.mutable


object DiskStorage {

  def main(args: Array[String]): Unit = {
    DiskStorage.init("/DATA0/x.txt")
  }

  def init(filename: String): Unit =
    val file = new RandomAccessFile(filename, "rw")
    val toPut = Array.fill[Byte](1024 * 1024 * 1024)(1)
    for (i <- 0 until 12)
      val mmb = file.getChannel.map(FileChannel.MapMode.READ_WRITE, i * 1024 * 1024 * 1024L, 1 * 1024 * 1024 * 1024L)
      mmb.put(toPut)
      mmb.position(0)
      mmb.get(toPut)


    file.close()
}

case class DiskStorageParams(
                              pageSize: Int = 128,
                              chunkSize: Int = 1024,
                            )

class DiskStorage(path: String, params: DiskStorageParams = DiskStorageParams()):
  private val file = new RandomAccessFile(path, "rw")

  private def makeChunk(id: Int): MappedByteBuffer =
    file.getChannel.map(FileChannel.MapMode.READ_WRITE, id * params.chunkSize, params.chunkSize)

  private val mappedBuffers = mutable.Buffer(makeChunk(0))
  private val eofPointer = AtomicLong(0)

  private def chunkId(p: Pointer): Int = (p >>> 32).toInt

  private def inChunkOffset(p: Pointer): Int = (p & 0xFFFFFFFF).toInt

  def getChunk(p: Pointer): MappedByteBuffer =
    val cId = chunkId(p)
    if (cId < mappedBuffers.size)
      mappedBuffers(cId)
    else this.synchronized {
      val c = makeChunk(mappedBuffers.size)
      mappedBuffers += c
      c
    }
  end getChunk


  def pageNew(data: Array[Byte]): Pointer =
    assert(data.length < params.pageSize)
    val ptr = eofPointer.getAndAdd(params.pageSize)
    val chunk = getChunk(ptr)
    val offset = inChunkOffset(ptr)
    chunk.put(offset, data)
    ptr
  end pageNew

  def pageGet(p: Pointer): Array[Byte] =
    val res = Array.ofDim[Byte](params.pageSize)
    getChunk(p).get(inChunkOffset(p), res)
    res
  end pageGet

  def pageDel(p: Pointer): Unit =
    ()
  end pageDel

end DiskStorage

