package server

import org.scalatest.{FlatSpec, Matchers}
import scala.util.Random
import akka.util.Timeout
import Utils.FileUtils._
import akka.actor.ActorSystem
import client.Messages._
import akka.pattern.ask
import scala.concurrent.Await
import client.Messages.Get
import client.Messages.Answer
import client.Messages.OK
import scala.concurrent.duration._
import client.Messages.NoKey
import client.Messages.Insert
import java.io.{FileReader, BufferedReader}

/**
 * User: Vasily
 * Date: 15.10.13
 * Time: 13:12
 */
class NodeGarbageCollectorsStress extends FlatSpec with Matchers {
  val rand = new Random()

  "storage" should "not crashed with a lot of flush operations" in {
    val path = "src/test/resources/flushTest/"
    removeFolder(path + "db")
    removeFolder(path)

    val timeout = Timeout(5000)
    val system = ActorSystem("NodeGarbageCollectorTest")
    val node = system.actorOf(server.Nodes.Node.props(path, 8))

    //val testLimit = 1000000
    val testLimit = 100000
    for (i <- 0 to testLimit) {
      val future = node.ask(Insert(i.toString, i.toString, i))(5 seconds)
      val result = Await.result(future, timeout.duration)
      result should be(OK("key inserted", i))
      if (i % 1000 == 0) node ! Flush()
      val id = rand.nextInt(i / 100 + 1)
      val futureGet = node.ask(Get(id.toString, id))(5 seconds)
      Await.result(futureGet, timeout.duration) should be(Answer(id.toString, id.toString, id))
      Thread.sleep(1)
    }

    for (i <- 0 to testLimit by 2) {
      val future = node.ask(Remove(i.toString, i))(5 seconds)
      val result = Await.result(future, timeout.duration)
      result should be(Removed(true, i))
      if (i % 500 == 0) node ! Flush()
      Thread.sleep(1)
    }

    for (i <- 0 to testLimit) {
      val futureGet = node.ask(Get(i.toString, i))(5 seconds)
      Await.result(futureGet, timeout.duration) should be(if (i % 2 != 0) Answer(i.toString, i.toString, i) else NoKey(i.toString, i))
    }

    node ! Close()
    system.shutdown()
  }
  "In nodex" should "garbage collector should work with big files and 1m queries" in {
    val path = "src/test/resources/stressNode/"
    removeFolder(path + "db")
    removeFolder(path)
    val valuePath = "src/test/resources/BigData/"
    val value = new BufferedReader(new FileReader(valuePath + "testline")).readLine()
    val timeout = Timeout(25000)
    val system = ActorSystem("NodeGarbageCollectorTest")
    val node = system.actorOf(server.Nodes.Node.props(path, 2))
    val testLimit = 1000000
    for (i <- 0 to testLimit) {
      val future = node.ask(Insert(i.toString, value + i.toString, i))(25 seconds)
      val result = Await.result(future, timeout.duration)
      result should be(OK("key inserted", i))

    }
    for (i <- 0 to testLimit by 2) {
      val future = node.ask(Remove(i.toString, i))(5 seconds)
      val result = Await.result(future, timeout.duration)
      result should be(Removed(true, i))
    }
    //wait for some garbage collection
    Thread.sleep(60000)

    for (i <- 0 to testLimit) {
      val futureGet = node.ask(Get(i.toString, i))(5 seconds)
      Await.result(futureGet, timeout.duration) should be(if (i % 2 != 0) Answer(i.toString, value + i.toString, i) else NoKey(i.toString, i))
    }
    Thread.sleep(1000)
    node ! Close()
    system.shutdown()
  }

}
