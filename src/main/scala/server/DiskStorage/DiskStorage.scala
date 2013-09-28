package server.DiskStorage

import server.Exception._
import scala.io._
import java.nio.file._
import java.io._
import server.Database


class DiskStorage(dbPath: String) extends Database {
  private val dbDir = if (dbPath.endsWith("/")) dbPath else dbPath + "/"
  start()

  def contains(key: String): Boolean = {
    if (Files.exists(Paths.get(dbDir + key))) true else false
  }

  def insert(key: String, value: String) {
    if (contains(key)) throw new KeyExistsException()
    else {
      try {
        val pw = new BufferedWriter(new FileWriter(dbDir + key))
        pw.write(value)
        pw.flush()
        pw.close()
      } catch {
        case e: IOException => throw new ReadKeyException
      }
    }
  }


  private def start() {
    if (!Files.exists(Paths.get(dbDir))) Files.createDirectory(Paths.get(dbDir))
  }


  def getFromFile(key: String): String = {

  }

  def get(key: String) = {
    if (!contains(key)) throw new NoKeyFoundException()
    try {
      var value = ""
      for (line <- Source.fromFile(dbDir + key).getLines()) {
        value += line
      }
      value
    } catch {
      case _: Throwable => throw new DatabaseReadException
    }
  }

  def update(key: String, value: String) {
    if (!contains(key)) throw new NoKeyFoundException()
    val oldKey = new File(dbDir + key)
    val backup = new File(dbDir + key + "." + System.currentTimeMillis())
    oldKey.renameTo(backup)
    try {
      insert(key, value)
      backup.delete()
    } catch {
      case e: ReadKeyException => {
        removeFile(dbDir + key)
        oldKey.renameTo(new File(dbDir + key))
      }
    }
  }

  def removeFile(path: String) {
    if (Files.exists(Paths.get(path)))
      (new File(path)).delete()
  }

  def remove(key: String) {
    if (!contains(key)) throw new NoKeyFoundException()
    try {
      val keyFile = new File(dbDir + key)
      keyFile.delete()
    } catch {
      case e: IOException => throw new KeyRemoveException()
    }
  }
}
