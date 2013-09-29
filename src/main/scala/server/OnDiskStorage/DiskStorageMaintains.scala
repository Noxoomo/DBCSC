package server.OnDiskStorage

import java.io.RandomAccessFile
import server.Exception.ReindexException
import server.Utils.FileUtils._


/**
 * User: Vasily
 * Date: 28.09.13
 * Time: 21:56
 */
class DiskStorageMaintains(database: String) {
  /**
   * restores after crash
   */
  def restore() {

  }

  /**
   * recreate database (removes old keys and values)
   */
  def clean() {
    val dbFile = new RandomAccessFile(database, "r")
    val writer = new RandomAccessFile(database + ".new", "w")

    var bytesRead = 0L
    try {
      while (bytesRead < dbFile.length()) {
        val blockSize = dbFile.readInt()
        val removed = dbFile.readBoolean()
        if (!removed) {
          val bytes = new Array[Byte](blockSize)
          bytesRead += dbFile.read(bytes)
          writer.write(bytes)
        }
      }
    } catch {
      case e: Throwable => throw new ReindexException()
    } finally {
      dbFile.close()
      writer.close()
    }
    removeFile(database)
    renameFile(database + ".new", database)
  }


}
