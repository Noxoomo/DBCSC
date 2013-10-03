package distributed


import org.scalatest.{Matchers, FlatSpec}
import akka.actor.ActorSystem
import client.Messages._
import scala.concurrent.duration._
import scala.concurrent.Await
import akka.util.Timeout

import akka.pattern.ask
import Utils.FileUtils._
import client.Messages.Get
import client.Messages.Answer
import scala.util.Random

/**
 * User: Vasily
 * Date: 03.10.13
 * Time: 14:22
 */
class RouterTest extends FlatSpec with Matchers {

  val rand = new Random()
  val timeout = Timeout(1000)
  val system = ActorSystem("RouterTest")
  val dbDir = "src/test/resources/distributedTests/ThreeNodeTest/"
  removeFolder(dbDir + "node1")
  removeFolder(dbDir + "node2")
  removeFolder(dbDir + "node3")
  val node1 = system.actorOf(server.Nodes.Node.props(dbDir + "node1"))
  val node2 = system.actorOf(server.Nodes.Node.props(dbDir + "node2"))
  val node3 = system.actorOf(server.Nodes.Node.props(dbDir + "node3"))
  val nodes = Array(node1.path.toString, node2.path.toString, node3.path.toString)
  val router = system.actorOf(client.Router.props(nodes), "router")

  "Router" should "route queries" in {

    val keyPre = "key-"
    val valuePre = "some value "
    val timeout = Timeout(1000)

    //val testLimit = 1000000
    val testLimit = 10000

    for (i <- 0 to testLimit) {
      val future = router.ask(Insert(keyPre + i.toString, valuePre + i.toString))(5 seconds)
      Await.result(future, timeout.duration) should be(OK("key inserted"))
      val id = rand.nextInt(i + 1)
      val futureGet = router.ask(Get(keyPre + id.toString))(5 seconds)
      Await.result(futureGet, timeout.duration) should be(Answer(keyPre + id.toString, valuePre + id.toString))
    }

  }


}
