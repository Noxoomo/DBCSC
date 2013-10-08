package client

import Messages._
import Messages.Response
import akka.actor.{Props, Actor}
import akka.util.Timeout
import scala.util.Random


/**
 * User: Vasily
 * Date: 01.10.13
 * Time: 20:40
 */
class Router(nodesInfo: Array[String]) extends Actor {
  val nodes = nodesInfo.map(x => context.actorSelection(x))
  val timeout = new Timeout(5000)
  val random = new Random()


  override def receive: Actor.Receive = {
    case Response() => {
      context.parent ! Response
    }
    case Close => {
      for (node <- nodes) node ! Close()
    }

    case Get(key, id) => {
      val node = getNodes(key)
      node ! Get(key, id)

    }
    case Remove(key, id) => {
      val node = getNodes(key)
      node ! Remove(key, id)
    }
    case Insert(key, value, id) => {
      val node = getNodes(key)
      node ! Insert(key, value, id)
    }

    case Update(key, value, id) => {
      val node = getNodes(key)
      node ! Update(key, value, id)
    }
    case _ => sender ! Error("unknown command", -1)
  }


  private def getNodes(key: String) = {
    nodes(abs(key.hashCode) % nodes.length)
  }

  private def abs(x: Int) = if (x > 0) x else -x

}

object Router {
  def props(nodes: Array[String]): Props = Props(classOf[Router], nodes)
}