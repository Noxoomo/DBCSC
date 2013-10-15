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
    } else {
      val mid: Int = files.length / 2
      val (leftFiles, rightFiles) = files.splitAt(mid)
      val left = merge(leftFiles)
      val right = merge(rightFiles)
      val result = merge(left, right)

      if (leftFiles.length > 1) removeFile(left) else if (leftFiles(0).getAbsolutePath.contains("merge")) removeFile(left)
      if (rightFiles.length > 1) removeFile(right) else if (rightFiles(0).getAbsolutePath.contains("merge")) removeFile(right)
      result
    }
  }

  def flush(memory: Memory): String = {
    if (pathExists(database + System.currentTimeMillis().toString)) Thread.sleep(5)
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
  def garbageCollect(bigIndex: Boolean = true): (RandomAccessFile, MappedByteBuffer) = {
    val oldFiles = getFileList
    val oldIndex = getOldIndexList
    if (oldFiles.length > 0) {
      val filename = merge(oldFiles)
      if (pathExists(database + System.currentTimeMillis())) Thread.sleep(5)
      val newName = database + System.currentTimeMillis()
      renameFile(filename, newName)
      removeOld(oldFiles, oldIndex)
      val indexBuffer = if (bigIndex) FileIndex.indexBigFiles(newName) else FileIndex.index(newName)
      val randAccessFile = new RandomAccessFile(newName, "r")
      (randAccessFile, indexBuffer)

    } else (null, null)
  }

  def garbageCollect(n: Int): ((RandomAccessFile, MappedByteBuffer), (Array[File], Array[File])) = {
    val oldFiles = getFileList.slice(0, n)
    val oldIndex = getOldIndexList.sortBy(x => x.getName.replace(".index", "").toLong).slice(0, n)
    if (oldFiles.length > 0) {
      val filename = merge(oldFiles)
      val newName = database + (oldFiles(n - 1).getName.toLong + 1).toString
      if (!renameFile(filename, newName)) {
        System.err.println("can't rename file, something went wrong")
        System.exit(1)
      }
      if (!pathExists(newName)) {
        System.err.println("Can't find created file, something went wrong")
        System.exit(1)
      }
      val indexBuffer = FileIndex.indexBigFiles(newName)
      val randAccessFile = new RandomAccessFile(newName, "r")
      ((randAccessFile, indexBuffer), (oldFiles, oldIndex))
    } else (null, null)
  }

  def removeOld(oldFiles: Array[File], oldIndex: Array[File]) = {
    for (file <- oldFiles) {
      removeFile(file.getAbsolutePath)
    }
    for (file <- oldIndex)
      removeFile(file.getAbsolutePath)
  }

  private def merge(firstName: String, secondName: String): String = {
    val first = new RandomAccessFile(firstName, "r")
    val second = new RandomAccessFile(secondName, "r")
    //first should be created before second
    val firstReader = new Reader(first)
    val secondReader = new Reader(second)
    if (pathExists(dbPath + "merge/" + System.currentTimeMillis().toString)) Thread.sleep(5)
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
