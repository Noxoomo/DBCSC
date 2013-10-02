package server

import org.scalatest._
import server.Exception.NoKeyFoundException
import scala.util.Random

import Utils.FileUtils._
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
    db.update("key1", "update-value1\n")
    db.get("key1") should be("update-value1\n")
    db.get("key2") should be("value2")
    db.remove("key2")
    db.remove("key1")
    intercept[NoKeyFoundException] {
      db.get("key2")
    }
    intercept[NoKeyFoundException] {
      db.get("key1")
    }
    db.close()
  }

  "DiskStorage" should "pass stress-test" in {
    val path = "src/test/resources/testStressDiskStorage/"
    val keyPre = "key-"
    val valuePre = "some value "
    //val testLimit = 1000000
    val testLimit = 100000
    removeFolder(path)
    val startTime = System.currentTimeMillis()

    val db = new DiskStorage(path)
    for (i <- 0 to testLimit) {
      db.insert(keyPre + i.toString, valuePre + i.toString)
      val id = rand.nextInt(i + 1)
      db.get(keyPre + id) should be(valuePre + id)
    }
    db.close()
    val endTime = System.currentTimeMillis()
    print(endTime - startTime)
  }

  "DiskStorage" should "work after clean" in {
    val path = "src/test/resources/testDiskStorageClean/"
    removeFile(path + "db")
    removeFile(path + "index")
    touch(path + "clean.lck")
    copyFile(path + "db.test", path + "db")
    copyFile(path + "index.test", path + "index")
    val db = new DiskStorage(path)
    intercept[NoKeyFoundException] {
      db.get("key1")
    }
    db.get("key2") should be("value-2")
    db.get("key3") should be("value-3")
    db.get("key4") should be("value4")
    db.close()
  }


  "DiskStorage" should "reindex in existing Database" in {
    val path = "src/test/resources/testExistDatabase"
    touch(path + "/index.lck")
    removeFile(path + "/index")
    val db = new DiskStorage(path)
    db.get("key1") should be("value1")
    db.get("key2") should be("value2")
    db.get("key3") should be("value3")
    db.get("key4") should be("value4")
    db.close()
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
    db.close()
  }


}
