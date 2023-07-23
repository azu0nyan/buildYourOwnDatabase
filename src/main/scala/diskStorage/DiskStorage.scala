package diskStorage

import btree.BTree.Pointer

import java.io.RandomAccessFile
import java.nio.{ByteBuffer, MappedByteBuffer}
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
                            ) {
}

class DiskStorage(path: String, params: DiskStorageParams = DiskStorageParams()):

  val printVerbose = false
  private val file = new RandomAccessFile(path, "rw")
  private val masterPage = Array.ofDim[Byte](params.pageSize)
  file.read(masterPage, 0, params.pageSize)

  private val mappedBuffersChunks = mutable.Buffer(makeChunk(0))
  private val flushed: Long = params.pageSize


  private val eofPointer = AtomicLong {
    val eof = ByteBuffer.wrap(masterPage)
      .position(0)
      .getLong(0)
    if (eof == 0) params.pageSize //new file
    else eof
  }
  println(s"Disk storage init finished.")
  println(s"Total space ${eofPointer.get} bytes. ${pagesAllocated} pages.")

  private def makeChunk(id: Int): MappedByteBuffer =
    file.getChannel.map(FileChannel.MapMode.READ_WRITE, id * params.chunkSize, params.chunkSize)

  def pagesAllocated: Long = eofPointer.get() / params.pageSize

  def isEmpty = pagesAllocated == 1 //only master page

  def writeMasterPage(userData: Array[Byte]): Unit =
    ByteBuffer.wrap(masterPage)
      .position(0)
      .putLong(eofPointer.get())
      .put(userData)
    val chunk = getChunk(0)
    chunk.put(0, masterPage)
    chunk.force(0, params.pageSize)
  end writeMasterPage

  def getUserData: ByteBuffer = ByteBuffer.wrap(masterPage).slice(8, params.pageSize - 8)

  private def chunkId(p: Pointer): Int = (p / params.chunkSize).toInt

  private def inChunkOffset(p: Pointer): Int = {
    (p  % params.chunkSize).toInt
  }

  def getChunk(p: Pointer): MappedByteBuffer =
    val cId = chunkId(p)
    if (cId < mappedBuffersChunks.size)
      mappedBuffersChunks(cId)
    else this.synchronized {
      val c = makeChunk(mappedBuffersChunks.size)
      mappedBuffersChunks += c
      c
    }
  end getChunk


  def pageNew(data: Array[Byte]): Pointer =
    assert(data.length <= params.pageSize)
    val ptr = eofPointer.getAndAdd(params.pageSize)
    val chunk = getChunk(ptr)
    val offset = inChunkOffset(ptr)
    chunk.put(offset, data)
    if(printVerbose) println(s"New page at $chunk $offset ptr $ptr ${data.mkString(",")}")
    ptr
  end pageNew

  def pageGet(ptr: Pointer): Array[Byte] =
    val res = Array.ofDim[Byte](params.pageSize)
    val chunk = getChunk(ptr)
    val offset = inChunkOffset(ptr)
    chunk.get(offset, res)
    if(printVerbose) println(s"Getting page for $ptr $chunk $offset ${res.mkString(",")}")
    res
  end pageGet

  def pageDel(p: Pointer): Unit =
    ()
  end pageDel

  def flushPages(): Unit =
    val startChunk = chunkId(flushed)
    val flushEnd = eofPointer.get()
    val end = chunkId(flushEnd)
    for {
      i <- startChunk to end if mappedBuffersChunks.size > i
      chunk = mappedBuffersChunks(i)
    } {
      if (i == startChunk && startChunk == end)
        val d = inChunkOffset(flushEnd) - inChunkOffset(startChunk)
        chunk.force(inChunkOffset(startChunk), d)
      else if (i == startChunk)
        chunk.force(inChunkOffset(flushed), chunk.capacity() - inChunkOffset(flushed))
      else if (i == end)
        chunk.force(0, inChunkOffset(flushEnd))
      else chunk.force()
    }

end DiskStorage

