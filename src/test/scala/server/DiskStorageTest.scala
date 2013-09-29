package server

import org.scalatest._
import server.Exception.NoKeyFoundException
import scala.util.Random

import server.Utils.FileUtils._
import server.OnDiskStorage.DiskStorage

class DiskStorageTest extends FlatSpec with Matchers {
  val rand = new Random()

  "DiskStorage" should "create OnDiskStorage and implement CRUD" in {
    //clean(filename)
    val path = "src/test/resources/test01DiskStorage"
    removeFolder(path)
    val db = new DiskStorage(path)
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

  "DiskStorage" should "pass stress-test" in {
    val path = "src/test/resources/testStressDiskStorage"
    val keyPre = "key-"
    val valuePre = "some value "
    //val testLimit = 1000000
    val testLimit = 100000
    removeFolder(path)
    val db = new DiskStorage(path)
    for (i <- 0 to testLimit) {
      db.insert(keyPre + i.toString, valuePre + i.toString)
      val id = rand.nextInt(i + 1)
      db.get(keyPre + id) should be(valuePre + id)
    }
  }

  "DiskStorage" should "read from existing Database" in {
    //clean(filename)
    val path = "src/test/resources/testExistDatabase"
    val db = new DiskStorage(path)
    db.get("key1") should be("value1")
    db.get("key2") should be("value2")
    db.get("key3") should be("value3")
    db.get("key4") should be("value4")
    intercept[NoKeyFoundException] {
      db.get("ssss")
    }
  }


}
