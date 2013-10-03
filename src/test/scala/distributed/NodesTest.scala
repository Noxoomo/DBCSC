package distributed

import org.scalatest.{Matchers, FlatSpec}
import akka.actor.ActorSystem
import client.Messages._
import scala.concurrent.duration._
import scala.concurrent.Await
import akka.util.Timeout

import akka.pattern.ask

/**
 * User: Vasily
 * Date: 02.10.13
 * Time: 18:17
 */
class NodesTest extends FlatSpec with Matchers {
  "Nodes" should "start and proceed queries" in {
    val dbDir = "src/test/resources/testExistDatabase/"
    val timeout = Timeout(1000)
    val system = ActorSystem("NodeTest")
    val node = system.actorOf(server.Nodes.Node.props(dbDir))

    //key1
    val future1 = node.ask(Get("key1"))(5 seconds)
    val result1 = Await.result(future1, timeout.duration)
    result1 should be(Answer("key1", "value1"))
    //key2
    val future2 = node.ask(Get("key2"))(5 seconds)
    val result2 = Await.result(future2, timeout.duration)
    result2 should be(Answer("key2", "value2"))
    //key1
    val future3 = node.ask(Get("key3"))(5 seconds)
    val result3 = Await.result(future3, timeout.duration)
    result3 should be(Answer("key3", "value3"))

    //non-exists key
    val future = node.ask(Get("non-existing key"))(5 seconds)
    val result = Await.result(future, timeout.duration)
    result should be(NoKey("non-existing key"))
  }

  ignore should "proceed all CRUD queries" in {
    val dbDir = "src/test/resources/testExistDatabase/"
    val timeout = Timeout(1000)
    val system = ActorSystem("NodeTest")
    val node = system.actorOf(server.Nodes.Node.props(dbDir))


  }


}
