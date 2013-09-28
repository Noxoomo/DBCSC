package server.DiskStorage

import server.Exception._
import java.io._
import server.Database

import server.Utils.FileUtils._

/**
 * Database Storage Format:
 * blocksize (4 bytes) removed key value, key+value+4 = blocksize]x all keys
 * removed = T/F
 *
 */

class DiskStorage(dbPath: String) extends Database {
  private val dbDir = if (dbPath.endsWith("/")) dbPath else dbPath + "/"
  private val intSize = 4
  private val storageLock = dbDir + "storage.lck"
  private val indexLock = dbDir + "index.lck"
  private val keyIndexFilename = dbDir + "index"
  private val dbFilename = dbDir + "db"
  private val maintainer = new DiskStorageMaintains(dbDir)
  //need to restore?
  if (!pathExists(dbDir)) createFolder(dbDir)
  if (lockExists()) maintainer.restore()
  //all good, start storage
  private val index = new KeyIndex(keyIndexFilename, indexLock)
  private val commits = new CommitLog()
  private val dbFile = new RandomAccessFile(dbFilename, "rw")


  private def lockExists() = pathExists(indexLock) && pathExists(storageLock)


  def contains(key: String): Boolean = {
    if (index contains key) true else false
  }

  def insert(key: String, value: String) {
    if (contains(key)) throw new KeyExistsException()
    else {
      val ind = dbFile.length()
      val data = (" F " + key + " " + value).getBytes()
      val len = data.length
      dbFile.seek(ind)
      commits.insert(key, value, ind)
      try {
        dbFile.writeInt(len)
        dbFile.write(data)
      } catch {
        case e: IOException => throw new InsertKeyException
      }
      index.insert(key, ind)
    }
  }


  def get(key: String) = {
    if (!contains(key)) throw new NoKeyFoundException()
    val ind = index.get(key)
    dbFile.seek(ind)
    try {
      val len = dbFile.readInt()
      val bytes = new Array[Byte](len)
      dbFile.read(bytes)
      val str = new String(bytes)
      val strSplit = str.split(" ", 4)
      strSplit(3)
    } catch {
      case e: IOException => throw new KeyReadException()
    }

  }

  def update(key: String, value: String) {
    if (!contains(key)) throw new NoKeyFoundException()
    commits.update(key, value, index.get(key))
    remove(key)
    insert(key, value)
  }


  def remove(key: String) {

    if (!contains(key)) throw new NoKeyFoundException()
    val pos = index.get(key)
    commits.remove(key, pos)
    index.remove(key)
    dbFile.seek(pos + intSize)
    try {
      dbFile.write(" T".getBytes())
    } catch {
      case e: IOException => throw new KeyRemoveException()
    }
  }

  def close() {
    index.close()
    removeFile(storageLock)
  }

}
