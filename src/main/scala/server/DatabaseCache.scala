package server

import scala.collection.mutable
import server.Exception.NoKeyFoundException


class DatabaseCache {
  private val db = new mutable.HashMap[String, String]()
  val time = 0

  def get(key: String): String = {
    if (!(db contains key)) throw new NoKeyFoundException
    db.get(key).get
  }

  def contains(key: String) = db contains key

  def update(key: String, value: String) {
    db.put(key, value)
  }

  def insert(key: String, value: String) {
    db.put(key, value)
  }

  def remove(key: String) {
    db.remove(key)
  }


}
