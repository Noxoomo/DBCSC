package server

import server.OnDiskStorage.DiskStorage
import server.Exception.NoKeyFoundException
import server.Traits.Database

/**
 * User: Vasily
 * Date: 28.09.13
 * Time: 20:18
 */
class Storage(dbPath: String) extends Database {
  //val dbMemory = new MemoryCache
  val dbStorage = new DiskStorage(dbPath)

  def contains(key: String): Boolean = {
    dbStorage contains key
  }


  def insert(key: String, value: String) {
    dbStorage.insert(key, value)
  }

  def busy = dbStorage.busy

  def get(key: String) = {
    // if (dbMemory contains key) dbMemory get key
    if (dbStorage contains key) {
      val value = dbStorage.get(key)
      //dbMemory.insert(key, value)
      value
    } else throw new NoKeyFoundException
  }


  def update(key: String, value: String) {
    if (!contains(key)) throw new NoKeyFoundException
    //dbMemory.remove(key)
    dbStorage.update(key, value)
  }


  def remove(key: String) {
    if (!contains(key)) throw new NoKeyFoundException
    //dbMemory.remove(key)
    dbStorage.remove(key)
  }

  def close() {
    dbStorage.close()
  }

  def flush() {
    dbStorage.flush()
  }
}
