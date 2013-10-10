package server.OnDiskStorage

import server.Exception._
import java.io._

import Utils.FileUtils._
import java.nio.MappedByteBuffer
import server.OnDiskStorage.DiskStatus._

/**
 * Database Storage Format:
 * blocksize (4 bytes) removed key value, key+value+4 = blocksize]x all keys
 * removed = T/F
 *
 */

class DiskStorage(dbPath: String) {
  private val dbDir = if (dbPath.endsWith("/")) dbPath else dbPath + "/"
  private val maintainer = new DiskStorageMaintains(dbDir)
  private val memoryLimit = 150 * 1024 * 1024L
  if (!pathExists(dbDir)) createFolder(dbDir)
  if (!pathExists(dbDir + "merge/")) createFolder(dbDir + "merge/")
  //files and their indexes
  val dbData = maintainer.garbageCollect()
  private var files = if (dbData._1 != null) List(dbData._1) else List()
  private var index = if (dbData._2 != null) List(dbData._2) else List()
  private val memory = maintainer.restore()


  //first run or need clean keys?


  //all good, start storage

  private var commits = new CommitLog(dbPath)

  def flush() {
    val filename = maintainer.flush(memory)
    index = FileIndex.index(filename) :: index
    files = new RandomAccessFile(filename, "r") :: files
    memory.clear()
    commits.close()
    commits = new CommitLog(dbPath)
  }


  def insert(key: String, value: String) {
    commits.insert(key, value)
    memory.insert(key, value)
    if (memory.getMemoryUsage > memoryLimit) {
      flush()
    }
  }


  private def look(lookupkey: String, files: List[RandomAccessFile], index: List[MappedByteBuffer]): DiskLookupResult = {
    if (index.isEmpty) NoKeyFound()
    else {
      val toLook = FileIndex.get(lookupkey, index.head)
      def proceed(toLook: List[Long]): DiskLookupResult = {
        if (toLook.isEmpty) NoKeyFound()
        else {
          val file = files.head
          file.seek(toLook.head)
          try {
            val keyLen = file.readInt()
            val keyBytes = new Array[Byte](keyLen)
            val removed = file.readBoolean()
            file.read(keyBytes)
            val key = new String(keyBytes)
            if (lookupkey == key) {
              if (removed) WasRemoved()
              else {
                val valueLen = file.readInt()
                val valueBytes = new Array[Byte](valueLen)
                file.read(valueBytes)
                FoundValue(new String(valueBytes))
              }
            } else proceed(toLook.tail)

          } catch {
            case e: IOException => throw new KeyReadException()
          }
        }
      }
      val result = proceed(toLook)
      result match {
        case NoKeyFound() => look(lookupkey, files.tail, index.tail)
        case _ => result
      }
    }
  }

  def get(key: String): StorageResponse = {
    if (memory contains key) Value(memory.get(key))
    else if (memory.wasRemoved(key)) NothingFound()
    else if (!index.isEmpty) {
      val result = look(key, files, index)
      result match {
        case FoundValue(value) => Value(value)
        case WasRemoved() => NothingFound()
        case NoKeyFound() => NothingFound()
      }
    } else NothingFound()
  }


  def update(key: String, value: String) {
    insert(key, value)
    // if (!contains(key)) throw new NoKeyFoundException()
    //remove(key)
    //insert(key, value)
  }


  def remove(key: String) {
    //if (!contains(key)) throw new NoKeyFoundException()
    commits.remove(key)
    memory.remove(key)
  }

  def close() {
    maintainer.flush(memory)
    commits.close()
  }
}

