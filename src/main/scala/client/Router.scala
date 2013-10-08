package client

import Messages._
import akka.actor.{Props, Actor}
import akka.pattern.ask
import scala.concurrent.duration._
import scala.concurrent.{TimeoutException, Await}
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
    case Close => {
      val futures = for (node <- nodes) yield node.ask(Close())(5 seconds)
      for (future <- futures) {
        Await.result(future, timeout.duration)
      }
      sender ! OK("quit")
    }
    case Get(key) => {
      val node = getNodes(key)
      val future = node.ask(Get(key))(5 seconds)
      try {
        val result = Await.result(future, timeout.duration)
        sender ! result
      } catch {
        case timeout: TimeoutException => sender ! Error("Timeout")
      }
    }
    case Remove(key) => {
      val node = getNodes(key)
      val future = node.ask(Remove(key))(5 seconds)
      try {
        val result = Await.result(future, timeout.duration)
        sender ! result
      } catch {
        case timeout: TimeoutException => sender ! Error("Timeout")
      }
    }
    case Insert(key, value) => {
      val node = getNodes(key)
      val future = node.ask(Insert(key, value))(5 seconds)
      try {
        val result = Await.result(future, timeout.duration)
        sender ! result

      } catch {
        case timeout: TimeoutException => sender ! Error("Timeout")
      }
    }

    case Update(key, value) => {
      val node = getNodes(key)
      val future = node.ask(Update(key, value))(5 seconds)
      try {
        val result = Await.result(future, timeout.duration)
        sender ! result
      } catch {
        case timeout: TimeoutException => sender ! Error("timeout")
      }
    }
    case _ => sender ! Error("unknown command")
  }


  private def getNodes(key: String) = {
    nodes(abs(key.hashCode) % nodes.length)
  }

  private def abs(x: Int) = if (x > 0) x else -x

}

object Router {
  def props(nodes: Array[String]): Props = Props(classOf[Router], nodes)
}