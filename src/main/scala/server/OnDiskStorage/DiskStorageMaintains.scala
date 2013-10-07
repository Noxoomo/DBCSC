package server.OnDiskStorage

import java.io._
import Utils.FileUtils._


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

  def getFileList() = new File(database).listFiles()
    .filter(x => !x.getName.endsWith(".merge")).sortBy(x => x.getName.toLong)

  def merge(files: Array[File]): List[RandomAccessFile] = {
    if (files.length > 1) {
      val readers = (for (file <- files) yield new RandomAccessFile(file.getAbsolutePath, "r")).toList
      val filename = database + System.currentTimeMillis().toString
      val file = new RandomAccessFile(filename + ".merge", "rw")
      for (index <- new KeyIndex(readers).index.values) {
        if (!index.removed) {
          index.file.seek(index.offset)
          val keyLen = index.file.readInt()
          val removed = index.file.readBoolean()
          val valueLen = index.file.readInt()
          val bytes = new Array[Byte](keyLen + valueLen)
          index.file.read(bytes)
          file.writeInt(keyLen)
          file.writeBoolean(removed)
          file.writeInt(valueLen)
          file.write(bytes)
        }
      }
      file.close()
      new File(filename + ".merge").renameTo(new File(filename))
      for (file <- files) removeFile(file.getAbsoluteFile.toString)
      List(new RandomAccessFile(filename, "r"))
    }
    else if (files.length == 1) {
      List(new RandomAccessFile(files(0), "r"))
    }
    else {
      null
    }
  }

  def flush(memory: Memory): RandomAccessFile = {
    val filename = database + System.currentTimeMillis().toString

    //val file = new RandomAccessFile(filename, "rws")
    val file = new DataOutputStream(new FileOutputStream(filename))
    for (key <- memory.getData.keySet) {
      val value = memory.get(key)
      file.writeInt(key.length)
      file.writeBoolean(false)
      file.writeInt(value.length)
      file.write((key + value).getBytes())
    }
    for (key <- memory.getRemoved) {
      file.writeInt(key.length)
      file.writeBoolean(true)
      file.write((key).getBytes())
    }
    file.flush();
    file.close()

    new RandomAccessFile(filename, "r")
  }

  /**
   * recreate database (removes old keys and values)
   */
  def clean(): List[RandomAccessFile] = merge(getFileList())


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
