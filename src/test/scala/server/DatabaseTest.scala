package server

import org.scalatest._
import java.io.File
import server.Exception.NoKeyFoundException
import scala.util.Random
import java.nio.file.{Paths, Files}


class DatabaseTest extends FlatSpec with Matchers {
  val rand = new Random()


  def clean(filename: String) {
    val commit = new File(filename + ".commit")
    if (commit.exists()) commit.delete()
    val db = new File(filename)
    if (db.exists()) db.delete()
  }

  def removeFolder(path: String): Boolean = {
    if (Files.exists(Paths.get(path))) {
      val dbRoot = new File(path)
      for (file <- dbRoot.listFiles()) {
        file.delete()
      }
      dbRoot.delete()
    }
    true
  }

  "Database" should "create database and implement CRUD" in {
    //clean(filename)
    val path = "src/test/resources/test02"
    removeFolder(path)
    val db = new Database(path)
    db.insert("key1", "value1")
    db.get("key1") should be("value1")
    db.insert("key2", "value2")
    db.update("key1", "update-value1")
    db.get("key1") should be("update-value1")
    db.get("key2") should be("value2")
    db.remove("key2")
    db.remove("key1")
    intercept[NoKeyFoundException] {
      db.get("key2")
    }
    intercept[NoKeyFoundException] {
      db.get("key1")
    }
  }


  "Database" should "implement read from existing source" in {
    //clean(filename)
    //createDatabase()
    val path = "src/test/resources/test01"
    val db = new Database(path)
    db.get("key") should be("value")
    db.get("key1") should be("value1")
    db.get("key2") should be("value2")
    db.get("key3") should be("value3")
    db.get("key4") should be("value4")
    intercept[NoKeyFoundException] {
      db.get("nokey")
    }
    intercept[NoKeyFoundException] {
      db.update("strange key", "value")
    }
  }

}
