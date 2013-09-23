package server

import scala.collection.mutable.Map
import server.Exception._
import scala.io._
import java.io.File
import java.nio.file._


class Database(filename: String) {
  private val db = Map[String, String]()
  private val dbFileName = filename
  private var started = false
  private var fw: java.io.PrintWriter = null


  def add(key: String, value: String) {
    if (!started) throw new DatabaseNotStartedException()
    if (db.contains(key)) throw new KeyExistsException()
    else {
      updateCommit(key, value)
      db += (key -> value)
    }
  }

  def backup() {

  }

  def open() {
    db.clear()
    try {
      for (line <- Source.fromFile(dbFileName).getLines()) {
        val request = line.toString.split("->")
        db.put(request(0), request(1))
      }
    } catch {
      case e: java.io.FileNotFoundException => println("no db found, creating new Database")
    }
    try {
      recoverCommitLog()
      backup()
      flush()
      clearCommitLog()
    } catch {
      case e: java.io.FileNotFoundException => println("no commit log found")
      case e: DataBaseOpenException => throw e
    }
  }

  def start() {
    if (!started) {
      open()
      recoverCommitLog()
      fw = new java.io.PrintWriter(new File(dbFileName + ".commit"))
      started = true
    }
  }

  def stop() {
    flush()
    if (fw != null) fw.close()
    clearCommitLog()
    started = false
  }

  def get(key: String) = {
    if (!started) throw new DatabaseNotStartedException()
    val answer = db.get(key)
    if (answer.isDefined) answer.get else throw new NoKeyFoundException()
  }

  def update(key: String, value: String) {
    if (!started) throw new DatabaseNotStartedException()
    updateCommit(key, value)
    db.update(key, value)

  }

  def remove(key: String) {
    if (!started) throw new DatabaseNotStartedException()
    if (db.contains(key)) {
      removeCommit(key)
      db.remove(key)
    } else throw new NoKeyFoundException
  }


  private def updateCommit(key: String, value: String) {
    commit("update " + key + "->" + value)

  }

  private def removeCommit(key: String) {
    commit("remove " + key)
  }

  private def commit(query: String) {
    try {
      fw.println(query)
    } catch {
      case _: Throwable => throw new CommitLogWriteException
    } finally {
      fw.flush()
    }
  }

  def flush() {
    val pw = new java.io.PrintWriter(new File(dbFileName))
    try {
      for ((key: String, value: String) <- db) {
        pw.println(key + "->" + value)
      }
    } catch {
      case _: Throwable => throw new DatabaseWriteException
    } finally {
      pw.flush()
      pw.close()
    }


  }

  private def clearCommitLog() {
    new File(dbFileName + ".commit").delete()
  }

  private def recoverCommitLog() {
    if (Files.exists(Paths.get(dbFileName + ".commit")))
      try {
        for (line <- Source.fromFile(dbFileName + ".commit").getLines()) {
          val request = line.toString.split(" ", 2)
          request(0) match {
            case "update" => {
              val keyValue = request(1).split("->")
              db.update(keyValue(0), keyValue(1))
            }
            case "remove" => db.remove(request(1))
          }
        }
      } catch {
        case _: Throwable => throw new RecoverCommitLogException
      }
  }
}
