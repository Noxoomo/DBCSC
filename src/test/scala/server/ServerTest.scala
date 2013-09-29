package server

import org.scalatest._
import server.Utils.FileUtils._

class ServerTest extends FlatSpec with Matchers {
  val filename = "src/test/resources/" + "serverTest"


  "Server" should "start database and process queries" in {
    removeFolder(filename)
    try {
      val server = new Server(filename)
      server.processLine("insert key1->value1") should be("inserted")
      server.processLine("get key1") should be("value1")
      server.processLine("insert key2->value2") should be("inserted")
      server.processLine("insert key2 value2") should be("Query syntax error")
      server.processLine("get key2") should be("value2")
      server.processLine("update key1->value-1") should be("updated")
      server.processLine("get key1") should be("value-1")
      server.processLine("remove key1") should be("removed")
      server.processLine("get key1") should be("No key found")
      server.processLine("insert key2->value3") should be("Error, key already exists")
    } finally {
      removeFolder(filename)
    }
  }


}
