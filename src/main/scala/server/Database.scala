package server

import server.Exception._
import scala.io._
import java.nio.file._
import java.io._


class Database(dbPath: String) {
  private val dbDir = if (dbPath.endsWith("/")) dbPath else dbPath + "/"
  private val dbCache = new DatabaseCache();
  start()

  def exist(key: String): Boolean = {
    if (dbCache.contains(key) || Files.exists(Paths.get(dbDir + key))) true else false
  }

  def insert(key: String, value: String) {
    if (exist(key)) throw new KeyExistsException()
    else {
      try {
        val pw = new BufferedWriter(new FileWriter(dbDir + key))
        pw.write(value)
        pw.flush()
        pw.close()
      } catch {
        case e: IOException => throw new DatabaseWriteException
      }
    }
  }


  private def start() {
    //clear hash
    /*dbHash.clear()
    if (Files.exists(Paths.get(dbDir))) {
      val dbRoot = new File(dbDir)
      try {
        for (file <- dbRoot.listFiles().filter(_.isFile)) {
          keySet += file.getName
        }
      } catch {
        case _: Throwable => println("something went wrong, check your permissions to access Database directory")
      }
    } else {
      Files.createDirectory(Paths.get(dbDir))
    }   */
    if (!Files.exists(Paths.get(dbDir))) Files.createDirectory(Paths.get(dbDir))
  }


  def getFromFile(key: String): String = {
    try {
      var value = ""
      for (line <- Source.fromFile(dbDir + key).getLines()) {
        value += line
      }
      dbCache.insert(key, value)
      value
    } catch {
      case _: Throwable => throw new DatabaseReadException
    }
  }

  def get(key: String) = {
    if (!(exist(key))) throw new NoKeyFoundException()
    if (dbCache contains key) dbCache get (key) else getFromFile(key)
  }

  def update(key: String, value: String) {
    if (!(exist(key))) throw new NoKeyFoundException()
    val oldKey = new File(dbDir + key)
    val backup = dbDir + key + "." + System.currentTimeMillis()
    oldKey.renameTo(new File(backup))
    try {
      dbCache.remove(key)
      insert(key, value)
    } catch {
      case e: DatabaseWriteException => {
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
    if (!exist(key)) throw new NoKeyFoundException()
    try {
      val keyFile = new File(dbDir + key)
      keyFile.delete()
      dbCache.remove(key)
    } catch {
      case e: IOException => throw new DatabaseKeyRemoveException()
    }
  }
}
