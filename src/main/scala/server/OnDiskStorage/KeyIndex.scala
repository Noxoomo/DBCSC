package server.OnDiskStorage

import java.io.RandomAccessFile
import server.Exception.{ReindexException, NoKeyFoundException}
import scala.collection.mutable


/**
 * User: Vasily
 * Date: 28.09.13
 * Time: 20:49
 */


class KeyIndex(files: List[RandomAccessFile]) {
  val index = new mutable.HashMap[String, Index]()

  if (files != null && !files.isEmpty) reIndex()

  def contains(key: String) = index.contains(key)

  def get(key: String): Index = {
    val answer = index.get(key)
    if (answer.isDefined) answer.get
    else throw new NoKeyFoundException
  }

  /**
   * reindex database
   */
  def indexFile(file: RandomAccessFile) = {
    var bytesRead = 0L
    file.seek(0)
    try {
      while (file.getFilePointer < file.length()) {
        val pos = file.getFilePointer
        val keySize = file.readInt()
        val removed = file.readBoolean()
        val valueSize = if (removed) 0 else file.readInt()
        val bytes = new Array[Byte](keySize)
        bytesRead += file.read(bytes)
        val key = new String(bytes)
        index.put(key, new Index(file, pos, removed))
        file.seek(file.getFilePointer + valueSize)
      }
    } catch {
      case e: Throwable => throw new ReindexException()
    }
  }

  def reIndex() {
    index.clear()
    files.foreach(indexFile)
  }
}

class Index(val file: RandomAccessFile, val offset: Long, val removed: Boolean) {
}
