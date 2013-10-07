package server.OnDiskStorage

import server.Traits.Database
import server.Exception.{NoKeyFoundException, KeyExistsException}

/**
 * User: Vasily
 * Date: 06.10.13
 * Time: 22:07
 */
class Memory extends Database {
  private var data = Map.empty[String, String] //new mutable.HashMap[String, String]

  private var removed = Set[String]()
  private var approximateMemory = 0L


  def getMemoryUsage: Long = approximateMemory

  def getData = data

  def getRemoved = removed

  def clear() {
    approximateMemory = 0
    data = Map.empty[String, String]
    removed = Set[String]()
  }

  private def memory(key: String, value: String): Long = {
    2 * (key.length + value.length)
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
  def insert(key: String, value: String): Unit =
    if (data contains key) throw new KeyExistsException
    else {
      approximateMemory += memory(key, value)
      data = data.+(key -> value)
      removed = removed.-(key)
    }

  /**
   * if key exists returns value of this key, otherwise throws NoKeyFoundException
   * @param key finds key in db and returns value
   * @throws NoKeyFoundException    if key doesn't exist
   *
   */
  def get(key: String): String = if (data contains key) data.get(key).get else throw new NoKeyFoundException


  def update(key: String, value: String): Unit = if (!data.contains(key)) throw new NoKeyFoundException
  else
    data = data.+(key -> value)

  /**
   *
   * @param key   to remove
   */
  def remove(key: String): Unit = {
    data = data.-(key)
    removed = removed.+(key)
  }
}
