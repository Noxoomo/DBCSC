package server

import scala.collection.mutable
import server.Exception.NoKeyFoundException


class DatabaseCache {
  private val db = new mutable.HashMap[String, String]()
  var time = 0
  var approximateMemory = 0
  val approximateMemoryLimit = 1024 * 1024 //1 GB


  def get(key: String): String = {
    if (!(db contains key)) throw new NoKeyFoundException
    db.get(key).get
  }

  def contains(key: String) = db contains key

  def update(key: String, value: String) {
    approximateMemory += value.length * 2
    approximateMemory -= db.get(key).get.length * 2
    db.put(key, value)
  }

  def insert(key: String, value: String) {
    approximateMemory += value.length * 2 + 2
    db.put(key, value)
  }

  def remove(key: String) {
    approximateMemory -= db.get(key).get.length * 2 - 2
    db.remove(key)
  }


}
