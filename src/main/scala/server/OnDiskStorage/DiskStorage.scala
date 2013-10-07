package server.OnDiskStorage

import server.Exception._
import java.io._

import Utils.FileUtils._
import server.Traits.Database

/**
 * Database Storage Format:
 * blocksize (4 bytes) removed key value, key+value+4 = blocksize]x all keys
 * removed = T/F
 *
 */

class DiskStorage(dbPath: String) extends Database {
  private val dbDir = if (dbPath.endsWith("/")) dbPath else dbPath + "/"
  private val maintainer = new DiskStorageMaintains(dbDir)
  private val memoryLimit = 80 * 1024 * 1024L
  private val files = maintainer.clean()
  var busy = false
  if (!pathExists(dbDir)) createFolder(dbDir)

  private val memory = maintainer.restore()


  //first run or need clean keys?


  //all good, start storage
  private val index = new KeyIndex(if (files != null) files.toList else null)
  private var commits = new CommitLog(dbPath)

  def flush() {
    index.indexFile(maintainer.flush(memory))
    memory.clear()
    commits.close()
    commits = new CommitLog(dbPath)
  }

  def contains(key: String): Boolean = {
    if (memory contains (key)) true
    else if (memory.wasRemoved(key)) false
    else if (index contains key) true else false
  }

  def insert(key: String, value: String) {
    if (contains(key)) throw new KeyExistsException()
    else {
      commits.insert(key, value)
      memory.insert(key, value)
      if (memory.getMemoryUsage > memoryLimit) {
        busy = true
        flush()
        busy = false
      }
    }
  }


  def get(key: String) = {
    if (memory contains (key)) memory.get(key)
    else if (memory.wasRemoved(key)) throw new NoKeyFoundException()
    else if (index contains (key)) {
      val ind = index.get(key)
      ind.file.seek(ind.offset)
      try {
        val keyLen = ind.file.readInt()
        val removed = ind.file.readBoolean()
        if (removed) throw new NoKeyFoundException
        val valueLen = ind.file.readInt()
        val bytes = new Array[Byte](keyLen + valueLen)
        ind.file.read(bytes)
        val str = new String(bytes)
        str.substring(keyLen)
      } catch {
        case e: IOException => throw new KeyReadException()
      }
    } else throw new NoKeyFoundException
  }


  def update(key: String, value: String) {
    if (!contains(key)) throw new NoKeyFoundException()
    remove(key)
    insert(key, value)
  }


  def remove(key: String) {
    if (!contains(key)) throw new NoKeyFoundException()
    commits.remove(key)
    memory.remove(key)
  }

  def close() {
    busy = true
    maintainer.flush(memory)
    commits.close()
  }
}
