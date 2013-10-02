package server.OnDiskStorage

import java.io.RandomAccessFile
import server.Exception.DatabaseCleanException
import Utils.FileUtils._


/**
 * User: Vasily
 * Date: 28.09.13
 * Time: 21:56
 */
class DiskStorageMaintains(dbDir: String) {
  val database = dbDir + "db"

  /**
   * recreate database (removes old keys and values)
   */
  def clean() {
    val dbFile = new RandomAccessFile(database, "r")
    val writer = new RandomAccessFile(database + ".new", "rw")

    var bytesRead = 0L
    try {
      while (dbFile.getFilePointer < dbFile.length()) {
        val pos = dbFile.getFilePointer
        val keySize = dbFile.readInt()
        val valueSize = dbFile.readInt()
        val removed = dbFile.readBoolean()
        if (!removed) {
          val bytes = new Array[Byte](keySize + valueSize)
          bytesRead += dbFile.read(bytes)
          writer.writeInt(keySize)
          writer.writeInt(valueSize)
          writer.writeBoolean(false)
          writer.write(bytes)
        } else dbFile.seek(dbFile.getFilePointer + keySize + valueSize)
      }


    } catch {
      case e: Throwable => throw new DatabaseCleanException()
    } finally {
      dbFile.close()
      writer.close()
    }
    removeFile(database)
    renameFile(database + ".new", database)
  }


}
