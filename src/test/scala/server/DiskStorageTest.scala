package server

import org.scalatest._
import server.Exception.NoKeyFoundException
import scala.util.Random

import Utils.FileUtils._
import server.OnDiskStorage.DiskStorage
import java.io.{FileReader, BufferedReader}

class DiskStorageTest extends FlatSpec with Matchers {
  val rand = new Random()

  "DiskStorage" should "create OnDiskStorage and implement CRUD" in {
    //clean(filename)
    val path = "src/test/resources/test01DiskStorage/"
    removeFolder(path + "db/")
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
    val testLimit = 10000
    removeFolder(path + "db/")
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

  "DiskStorage" should "recover from commit-log" in {
    val path = "src/test/resources/testDiskStorageClean/"
    removeFolder(path + "db/")
    createFolder(path + "db/")
    removeFile(path + "commits")
    copyFile(path + "commits.test", path + "commits")
    copyFile(path + "1381129040293", path + "db/1381129040293")

    val db = new DiskStorage(path)
    intercept[NoKeyFoundException] {
      db.get("key1")
    }
    db.get("key2") should be("value2")
    db.get("key3") should be("value3")
    db.get("key4") should be("value4")
    db.close()
  }



  "DiskStorage" should "read from existing Database" in {
    //clean(filename)
    val path = "src/test/resources/testExistDB/"
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

  "DiskStorage" should "handling big data" in {
    val path = "src/test/resources/BigData/"
    val db = new DiskStorage(path)
    val testLimit = 1000000
    val value = new BufferedReader(new FileReader(path + "testline")).readLine()
    for (i <- 0 to testLimit) {
      db.insert(i.toString, value + i.toString)
    }

    for (i <- 0 to testLimit) {
      db.get(i.toString) should be(value + i.toString)
    }
    db.close()
  }


}
