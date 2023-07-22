package db

class DiskKV() extends KV {
  
  override def get(key: Array[Byte]): Option[Array[Byte]] = ???
  override def set(key: Array[Byte], value: Array[Byte]): Unit = ???
  override def delete(key: Array[Byte]): Boolean = ???
}
