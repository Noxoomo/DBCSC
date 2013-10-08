package server.OnDiskStorage

import server.Exception._
import java.io._

import Utils.FileUtils._
import server.Traits.Database
import java.nio.MappedByteBuffer

/**
 * Database Storage Format:
 * blocksize (4 bytes) removed key value, key+value+4 = blocksize]x all keys
 * removed = T/F
 *
 */

class DiskStorage(dbPath: String) extends Database {
  private val dbDir = if (dbPath.endsWith("/")) dbPath else dbPath + "/"
  private val maintainer = new DiskStorageMaintains(dbDir)
  private val memoryLimit = 100 * 1024 * 1024L

  //files and their indexes
  private var files = List(maintainer.garbageCollect)
  private var index = List[MappedByteBuffer]()

  if (!pathExists(dbDir)) createFolder(dbDir)

  private val memory = maintainer.restore()


  //first run or need clean keys?


  //all good, start storage

  private var commits = new CommitLog(dbPath)

  def flush() {
    val filename = maintainer.flush(memory)
    index = (FileIndex.index(filename)) :: index
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

  private case class DiskLookupResult()

  private case class FoundValue(value: String) extends DiskLookupResult

  private case class WasRemoved() extends DiskLookupResult

  private case class NoKeyFound() extends DiskLookupResult

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
            file.read(keyBytes)
            val key = new String(keyBytes)
            if (lookupkey == key) {
              val removed = file.readBoolean()
              if (removed) WasRemoved()
              else {
                val valueLen = file.readInt()
                val valueBytes = new Array[Byte](valueLen)
                FoundValue(new String(valueBytes))
              }
            } else proceed(toLook.tail)

          } catch {
            case e: IOException => throw new KeyReadException()
          }
        }
      }
      proceed(toLook)
    }


    def get(key: String): StorageResponse = {
      if (memory contains (key)) Value(memory.get(key))
      else if (memory.wasRemoved(key)) NothingFound()
      else {
        val result = look(key, files, index)
        result match {
          case FoundValue(value) => Value(value)
          case WasRemoved => NothingFound()
          case NoKeyFound => NothingFound()
        }
      }
    }
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

case class StorageResponse()

case class Value(value: String) extends StorageResponse

case class NothingFound() extends StorageResponse
