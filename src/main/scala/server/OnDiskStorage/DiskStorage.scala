package server.OnDiskStorage

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
  private val cleanLock = dbDir + "clean.lck"
  private val keyIndexFilename = dbDir + "index"
  private val dbFilename = dbDir + "db"
  private val maintainer = new DiskStorageMaintains(dbFilename)
  //first run, need to restore or only clean keys?
  if (!pathExists(dbDir)) createFolder(dbDir)
  else if (pathExists(storageLock)) {
    maintainer.restore()
    touch(indexLock)
    removeFile(storageLock)
  } else if (pathExists(cleanLock)) {
    maintainer.clean()
    touch(indexLock)
    removeFile(cleanLock)
  }


  //all good, start storage
  private val index = new KeyIndex(keyIndexFilename, dbFilename, indexLock)
  private val commits = new CommitLog()
  private val dbFile = new RandomAccessFile(dbFilename, "rw")
  private var removedOperations = 0L


  def contains(key: String): Boolean = {
    if (index contains key) true else false
  }

  def insert(key: String, value: String) {
    if (contains(key)) throw new KeyExistsException()
    else {
      val ind = dbFile.length()
      val data = (key + " " + value).getBytes
      val len = data.length
      dbFile.seek(ind)
      commits.insert(key, value, ind)
      try {
        dbFile.writeInt(len)
        dbFile.writeBoolean(false)
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
      val removed = dbFile.readBoolean()
      if (removed) throw new NoKeyFoundException
      val bytes = new Array[Byte](len)
      dbFile.read(bytes)
      val str = new String(bytes)
      val strSplit = str.split(" ", 2)
      strSplit(1)
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
    if (removedOperations == 0) touch(cleanLock)
    removedOperations += 1
    val pos = index.get(key)
    commits.remove(key, pos)
    index.remove(key)
    dbFile.seek(pos + intSize)
    try {
      dbFile.writeBoolean(true)
    } catch {
      case e: IOException => throw new KeyRemoveException()
    }
  }

  def close() {
    index.close()
    removeFile(storageLock)
  }

}
