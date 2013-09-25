package server

import org.scalatest._
import java.io.File


class ServerTest extends FlatSpec with Matchers {
  val filename = "/Users/Vasily/Dropbox/IdeaProjects/DBCSC/src/test/resources/" + "data"

  def clean(filename: String) {
    val commit = new File(filename + ".commit")
    if (commit.exists()) commit.delete()
    val db = new File(filename)
    if (db.exists()) db.delete()
  }

  "Server" should "start database and process queries" in {
    //clean rubbish
    clean(filename)

    try {
      val server = new Server(filename)
      server.processLine("insert key1->value1") should be("inserted")
      server.processLine("get key1") should be("value1")
      server.processLine("insert key2->value2") should be("inserted")
      server.processLine("insert key2 value2") should be("Query syntax error")
      server.processLine("get key2") should be("value2")
      server.processLine("update key1->value-1") should be("updated")
      server.processLine("get key1") should be("value-1")
      server.processLine("flush") should be("flush done")
      server.processLine("remove key1") should be("removed")
      server.processLine("get key1") should be("No key found")
      server.processLine("insert key2->value3") should be("Error, key already exists")
      server.processLine("get key1") should be("Error, database stopped")
    } finally {
      clean(filename)
    }

  }


}
