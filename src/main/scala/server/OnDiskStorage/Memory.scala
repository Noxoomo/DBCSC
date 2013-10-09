package server.OnDiskStorage


import scala.collection.mutable

/**
 * User: Vasily
 * Date: 06.10.13
 * Time: 22:07
 */
class Memory {
  private var data = new mutable.HashMap[String, String] //new mutable.HashMap[String, String]

  private var removed = Set[String]()
  private var approximateMemory = 0L


  def getMemoryUsage: Long = approximateMemory

  def getData = data

  def getRemoved = removed

  def clear() {
    approximateMemory = 0
    data.clear()
    removed = Set[String]()
  }

  private def memory(key: String, value: String): Long = {
    (key.getBytes.length + value.getBytes.length)
  }

  /**
   * if key exists — return true, otherwise false
   * @param key for check
   * @return true if key exists
   */
  def contains(key: String): Boolean = data contains key

  def wasRemoved(key: String) = removed contains key

  /**
   * add key-value pair to storage
   * @param key  for insert
   * @param value for insert
   * @throws KeyExistsException  if key exists
   */
  def insert(key: String, value: String) {
    approximateMemory += memory(key, value)
    data.put(key, value)
    removed = removed.-(key)
  }

  /**
   * if key exists returns value of this key, otherwise throws NoKeyFoundException
   * @param key finds key in db and returns value
   * @throws NoKeyFoundException    if key doesn't exist
   *
   */
  def get(key: String): String = data.get(key).get


  def update(key: String, value: String) = insert(key, value)

  /**
   *
   * @param key   to remove
   */
  def remove(key: String): Unit = {
    data.remove(key)
    removed = removed.+(key)
  }
}
