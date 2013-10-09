package server.OnDiskStorage

import java.io._
import Utils.FileUtils._
import java.nio.MappedByteBuffer


/**
 * User: Vasily
 * Date: 28.09.13
 * Time: 21:56
 */
class DiskStorageMaintains(dbPath: String) {
  private val database = dbPath + "db/"
  private val infoSize = 4 * 2
  private val commitsFilename = dbPath + "commits"
  if (!pathExists(database)) createFolder(database)

  def getFileList = {
    val dir = new File(database).listFiles()
    dir.filter(x => !(x.getName.startsWith(".") || x.isDirectory || x.getName.endsWith(".index"))).sortBy(x => x.getName.toLong)
  }

  def getOldIndexList = new File(database).listFiles().filter(x => !x.isDirectory && x.getName.endsWith(".index"))

  private def merge(files: Array[File]): String = {
    if (files.length == 1) {
      files(0).getAbsolutePath
    } else if (files.length == 2) {
      merge(files(0).getAbsolutePath, files(1).getAbsolutePath)
    } else {
      val mid: Int = files.length / 2
      val left = merge(files.slice(0, mid))
      val right = merge(files.slice(mid, files.length))
      val result = merge(left, right)
      removeFile(left)
      removeFile(right)
      result
    }
  }

  def flush(memory: Memory): String = {
    val filename = database + System.currentTimeMillis().toString
    val keys = memory.getData.keySet.union(memory.getRemoved).toArray.sorted
    if (keys.length > 0) {
      val file = new DataOutputStream(new FileOutputStream(filename))
      for (key <- keys) {
        if (memory contains key) {
          val value = memory.get(key)
          val keyBytes = key.getBytes
          val valueBytes = value.getBytes
          file.writeInt(keyBytes.length)
          file.writeBoolean(false)
          file.write(keyBytes)
          file.writeInt(valueBytes.length)
          file.write(valueBytes)
        } else {
          val keyBytes = key.getBytes
          file.writeInt(keyBytes.length)
          file.writeBoolean(true)
          file.write(keyBytes)
        }
      }
      file.writeInt(-1)
      file.flush()
      file.close()
      filename
    } else null
  }

  /**
   * recreate database (removes old keys and values)
   */
  // def clean(): List[RandomAccessFile] = merge(getFileList())
  def garbageCollect(): (RandomAccessFile, MappedByteBuffer) = {
    val oldFiles = getFileList
    if (oldFiles.length > 0) {
      val filename = merge(oldFiles)
      val newName = database + System.currentTimeMillis()
      renameFile(filename, newName)
      for (file <- oldFiles) {
        removeFile(file.getAbsolutePath)
      }
      for (file <- getOldIndexList)
        removeFile(file.getAbsolutePath)
      val indexBuffer = FileIndex.index(newName)
      val randAccessFile = new RandomAccessFile(newName, "r")
      (randAccessFile, indexBuffer)

    } else (null, null)
  }

  private def merge(firstName: String, secondName: String): String = {
    val first = new RandomAccessFile(firstName, "r")
    val second = new RandomAccessFile(secondName, "r")
    //first should be created before second
    val firstReader = new Reader(first)
    val secondReader = new Reader(second)
    val filename = dbPath + "merge/" + System.currentTimeMillis().toString
    val writer = new DataOutputStream(new FileOutputStream(filename))

    firstReader.next()
    secondReader.next()
    while (firstReader.notEmpty || secondReader.notEmpty) {
      if (!firstReader.notEmpty && secondReader.notEmpty) {
        secondReader.write(writer)
        secondReader.next()
      } else if (!secondReader.notEmpty && firstReader.notEmpty) {
        firstReader.write(writer)
        firstReader.next()
      }
      else if (firstReader.current().key < secondReader.current().key) {
        if (firstReader.current().removed)
          firstReader.next()
        else {
          firstReader.write(writer)
          firstReader.next()
        }
      } else if (firstReader.current().key == secondReader.current().key) {
        if (!secondReader.current().removed) secondReader.write(writer)
        firstReader.next()
        secondReader.next()
      }
      else {
        secondReader.write(writer)
        secondReader.next()
      }
    }
    writer.writeInt(-1)
    writer.flush()
    writer.close()
    filename
  }

  def restore(): Memory = {
    if (!pathExists(commitsFilename)) new Memory
    else {
      val reader = new BufferedReader(new FileReader(dbPath + CommitLog.fileName))
      val memory = new Memory()
      try {
        var eof = false
        while (!eof) {
          val key = reader.readLine()
          if (key == null) eof = true
          else {
            if (key.endsWith("->")) {
              memory.remove(key.substring(0, key.length - 2))
            } else {
              val valueLen = reader.readLine().toInt
              val bytes = new Array[Char](valueLen)
              reader.read(bytes)
              reader.readLine()
              memory.insert(key, new String(bytes))
            }
          }
        }
      }
      catch {
        case e: IOException => throw e
      } finally {
        reader.close()
      }
      memory
    }
  }
}
