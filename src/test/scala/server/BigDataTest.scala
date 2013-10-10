package server

import org.scalatest._

import server.OnDiskStorage.DiskStorage
import java.io.{FileReader, BufferedReader}
import server.OnDiskStorage.DiskStatus._

/**
 * User: Vasily
 * Date: 10.10.13
 * Time: 20:06
 */
class BigDataTest extends FlatSpec with Matchers {
  //"Storage" should "handling big data" in {
  ignore should "handling big data" in {
    val path = "src/test/resources/BigData/"
    val db = new DiskStorage(path)
    val testLimit = 16000000
    val value = new BufferedReader(new FileReader(path + "testline")).readLine()
    for (i <- 0 to testLimit) {
      db.insert(i.toString, value + i.toString)
    }

    for (i <- 0 to testLimit) {
      db.get(i.toString) should be(Value(value + i.toString))
    }
    db.close()
  }


}
