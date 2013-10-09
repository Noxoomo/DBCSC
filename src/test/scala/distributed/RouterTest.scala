package distributed


import org.scalatest.{Matchers, FlatSpec}
import akka.actor.{Props, Actor, ActorSystem}
import client.Messages._
import akka.util.Timeout

import Utils.FileUtils._
import client.Messages.Get
import client.Messages.Answer
import scala.util.Random

/**
 * User: Vasily
 * Date: 03.10.13
 * Time: 14:22
 */
object ClientSurrogate {
  def props(nodes: Array[String]): Props = Props(classOf[ClientSurrogate], nodes)
}

class ClientSurrogate(nodes: Array[String]) extends FlatSpec with Actor with Matchers {
  val testLimit = 10000
  var inserted = 0
  var wasError = false
  var quit = 0
  val router = context.actorOf(client.Router.props(nodes), "router")

  def receive: Actor.Receive = {
    case Answer(key, value, id) => (key, value) should be(id.toString, id)
    case OK(text, id) => inserted += 1; OK(text, id) should be(OK("key inserted", id))
    case GetClose() => {
      quit += 1
      if (quit == nodes.length) {
        inserted should be(testLimit)
        context.system.shutdown()
      }
    }
    case "quit" => {
      router ! Close()
    }
    case Insert(key, value, id) => router ! Insert(key, value, id)
    case Get(key, id) => router ! Get(key, id)
    case _ =>

  }
}

class RouterTest extends FlatSpec with Matchers {


  val rand = new Random()
  val timeout = Timeout(1000)
  val system = ActorSystem("RouterTest")
  val dbDir = "src/test/resources/distributedTests/ThreeNodeTest/"
  removeFolder(dbDir + "node1/db")
  removeFolder(dbDir + "node2/db")
  removeFolder(dbDir + "node3/db")
  removeFolder(dbDir + "node1")
  removeFolder(dbDir + "node2")
  removeFolder(dbDir + "node3")
  val node1 = system.actorOf(server.Nodes.Node.props(dbDir + "node1/"))
  val node2 = system.actorOf(server.Nodes.Node.props(dbDir + "node2/"))
  val node3 = system.actorOf(server.Nodes.Node.props(dbDir + "node3/"))
  val nodes = Array(node1.path.toString, node2.path.toString, node3.path.toString)
  val testClient = system.actorOf(ClientSurrogate.props(nodes), "clientSurrogate")


  "Router" should "route queries" in {


    //val testLimit = 1000000
    val testLimit = 10000

    for (i <- 0 to testLimit) {
      testClient ! Insert(i.toString, i.toString, i)
    }
    for (i <- 0 to testLimit) {
      val id = rand.nextInt(i + 1)
      testClient ! Get(id.toString, id)
    }
    testClient ! "quit"
    system.awaitTermination()
  }


}
