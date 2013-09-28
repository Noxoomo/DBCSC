package server.MemoryStorage

import scala.collection.mutable
import server.Exception.NoKeyFoundException


class MemoryCache {
  private val db = new mutable.HashMap[String, (String, Long)]()
  var time = 0L
  var approximateMemory = 0L
  val approximateMemoryLimit = 512 * 1024L
  //≈512 MB
  val cleanRate = 0.7
  val memoryHeuristic = 5L


  def get(key: String): String = {
    if (!(db contains key)) throw new NoKeyFoundException
    val answer = db.get(key).get
    db.put(key, (answer._1, answer._2 + 1))
    time += 1
    answer._1
  }

  def contains(key: String) = db contains key

  private def checkMemoryUsage() {
    if (approximateMemory > cleanRate * approximateMemoryLimit) {
      clean()
    }
  }

  private def clean() {
    val oldTime = time
    time = 0
    approximateMemory = 0
    for ((key, value) <- db) {
      if (value._2 > oldTime * cleanRate) {
        db.put(key, (value._1, time))
        approximateMemory += memory(key, value)
      } else db.remove(key)

    }

  }

  private def memory(key: String, value: String): Long = {
    2 * (key.length + value.length + memoryHeuristic)
  }

  private def memory(key: String, value: (String, Long)): Long = {
    memory(key, value._1)
  }

  def update(key: String, value: String) {
    approximateMemory += memory(key, db.get(key).get._1)
    approximateMemory -= memory(key, value)
    db.put(key, (value, time))
    time += 1
    checkMemoryUsage()
  }

  def insert(key: String, value: String) {
    approximateMemory += memory(key, value)
    db.put(key, (value, time))
    time += 1
    checkMemoryUsage()
  }

  def remove(key: String) {
    approximateMemory -= (memory(key, db.get(key).get._1))
    db.remove(key)
  }


}
