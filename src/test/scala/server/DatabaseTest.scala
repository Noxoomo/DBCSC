package server

import org.scalatest._
import java.io.{PrintWriter, File}
import server.Exception.NoKeyFoundException
import scala.util.Random


class DatabaseTest extends FlatSpec with Matchers {
  val rand = new Random()
  val filename = "/Users/Vasily/Dropbox/IdeaProjects/DBCSC/src/test/resources/" + "Database" + rand.nextInt().toString


  def clean(filename: String) {
    val commit = new File(filename + ".commit")
    if (commit.exists()) commit.delete()
    val db = new File(filename)
    if (db.exists()) db.delete()
  }

  "Database" should "start with empty file" in {
    //clean(filename)
    val db = new Database(filename)
    db.insert("aaa", "bbb")
    db.get("aaa") should be("bbb")
    db.insert("someone", "33333")
    db.update("aaa", "ccc")
    db.get("aaa") should be("ccc")
    db.get("someone") should be("33333")
    db.remove("aaa")
    intercept[NoKeyFoundException] {
      db.get("aaa")
    }
  }


  ignore should "implement CRUD" in {
    //clean(filename)
    //createDatabase()
    val db = new Database(filename)
    db.get("key") should be("value")
    db.get("ppp") should be("lll 111")
    db.update("key1", "sss")
    db.get("key1") should be("sss")
    db.remove("ppp")

    intercept[NoKeyFoundException] {
      db.get("ppp")
      db.update("strange key", "value")
    }
    intercept[NoKeyFoundException] {
      db.remove("keysssss")
    }
  }


  def createDatabase() {
    val base = "key->value\nkey1->aaa\naaa->ddd\nd->k\nppp->lll 111\nsomeone-> +77777777"
    val db = new PrintWriter(new File(filename))
    db.append(base)
    db.flush()
    db.close()
  }

  def createCommitLog() {
    val log = "update key1->value1\nupdate key2->value2\nupdate test->tset\nremove aaa"
    val commitLog = new java.io.PrintWriter(new File(filename + ".commit"))
    commitLog.append(log)
    commitLog.flush()
    commitLog.close()
  }


}
