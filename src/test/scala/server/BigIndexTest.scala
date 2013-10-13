package server

/**
 * User: Vasily
 * Date: 13.10.13
 * Time: 17:37
 */

import org.scalatest._

import Utils.FileUtils._
import server.OnDiskStorage.DiskStorage
import server.OnDiskStorage.DiskStatus._

class BigIndexTest extends FlatSpec with Matchers {
  val path = "src/test/resources/BigIndex/"

  "Storage" should "index files with many keys" in {
    removeFolder(path + "db")
    val db = new DiskStorage(path)
    val testLimit = 1000013
    for (i <- 0 to testLimit) {
      db.insert(i.toString, i.toString)
    }

    db.close()
    val readDb = new DiskStorage(path)
    for (i <- 0 to testLimit) {
      readDb.get(i.toString) should be(Value(i.toString))
    }
    readDb.close()
  }
}
