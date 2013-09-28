package server

import org.scalatest._
import server.Exception.NoKeyFoundException
import scala.util.Random

import server.Utils.FileUtils._

class DiskStorageTest extends FlatSpec with Matchers {
  val rand = new Random()

  "DiskStorage" should "create DiskStorage and implement CRUD" in {
    //clean(filename)
    val path = "src/test/resources/test01"
    removeFolder(path)
    val db = new Storage(path)
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

  "DiskStorage" should "read from existing Database" in {
    //clean(filename)
    val path = "src/test/resources/test02DiskStorage"
    val db = new Storage(path)
    db.get("key1") should be("value1")
    db.get("key2") should be("value2")
    db.get("key3") should be("value3")
    db.get("key4") should be("value4")
    intercept[NoKeyFoundException] {
      db.get("ssss")
    }
  }


}
