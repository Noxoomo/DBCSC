package server.OnDiskStorage

import java.io.{IOException, FileReader, BufferedReader, RandomAccessFile}
import server.Exception.{KeyRemoveException, DatabaseCleanException}
import Utils.FileUtils._
import scala.io.Source


/**
 * User: Vasily
 * Date: 28.09.13
 * Time: 21:56
 */
class DiskStorageMaintains(dbDir: String) {
  val database = dbDir + "db"
  val infoSize = 4 * 2

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

  def restore() {
    val reader = new BufferedReader(new FileReader(dbDir + CommitLog.fileName))
    val dbFile = new RandomAccessFile(database, "rw")
    var eof = false
    while (!eof) {
      val key = reader.readLine
      val info = reader.readLine().split(" ")
      val index = info(0).toLong
      val keyLength = info(1).toInt
      val valueLength = info(2).toInt
      val valueArr = new Array[Char](valueLength)
      if (reader.read(valueArr) == -1) eof = true
      val value = new String(valueArr)
      if (!eof) //if -1  — we crashed during write key-value, 	 What a pity!
        try {
          dbFile.seek(index)
          val dbKeyLen = dbFile.readInt()
          val dbValLen = dbFile.readInt()
          val status = dbFile.readBoolean()
          val dbKeyBytes = new Array[Byte](dbKeyLen)
          val dbValBytes = new Array[Byte](dbValLen)
          dbFile.read(dbKeyBytes)
          dbFile.read(dbValBytes)
          //some error in db
          if (keyLength != dbKeyLen || valueLength != dbValLen ||
            value != new String(dbValBytes) || key != new String(dbKeyBytes)) {
            throw new Exception("Some error in db")

          }
        } catch {
          case e: Exception => {
            //we should restore db from this point
            dbFile.seek(index)
            dbFile.writeInt(keyLength)
            dbFile.writeInt(value.length)
            dbFile.writeBoolean(false)
            val data = (key + value).getBytes
            dbFile.write(data)

            while (!eof) {
              val info = reader.readLine().split(" ")
              val index = info(0).toLong
              val keyLength = info(1).toInt
              val valueLength = info(2).toInt
              val valueArr = new Array[Char](valueLength)
              if (reader.read(valueArr) == -1) eof = true
              if (!eof) {
                val value = new String(valueArr)
                dbFile.seek(index)
                dbFile.writeInt(keyLength)
                dbFile.writeInt(value.length)
                dbFile.writeBoolean(false)
                val data = (key + value).getBytes
                dbFile.write(data)
              }
            }
          }
        } finally {
          reader.close()
        }
    }
    for (line <- Source.fromFile(dbDir + "commits.remove")) {
      val index = line.toLong
      dbFile.seek(index + infoSize)
      try {
        dbFile.writeBoolean(true)
      } catch {
        case e: IOException => throw new KeyRemoveException()
      }
    }
    dbFile.close()
  }


}
