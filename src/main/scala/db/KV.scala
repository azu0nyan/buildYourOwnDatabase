package db

trait KV {
  def get(key: Array[Byte]): Option[Array[Byte]]
  def set(key: Array[Byte], value: Array[Byte]): Unit 
  def delete(key: Array[Byte]): Boolean 
}
