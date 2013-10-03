package client

import Messages._
import akka.actor.{Props, Actor}
import akka.pattern.ask
import scala.concurrent.duration._
import scala.concurrent.{TimeoutException, Await}
import akka.util.Timeout


/**
 * User: Vasily
 * Date: 01.10.13
 * Time: 20:40
 */
class Router(nodesInfo: Array[String]) extends Actor {
  val nodes = nodesInfo.map(x => context.actorSelection(x))
  //val nodes = Array(context.actorFor("akka://Node@127.0.0.1:2511/User/Storage"))
  //val nodes = nodesInfo.map(x => context.actorSelection(x))
  val timeout = new Timeout(1000)


  def getResult(result: Any): String = {
    result match {
      case Removed(done) => if (done) "removed" else "some error"
      case Answer(key, value) => "for " + key + " last record is " + value
      case OK(text) => text
      case Error(key) => "Error " + key
      case NoKey(key) => "No key found " + key
    }
  }

  override def receive: Actor.Receive = {
    case null =>
    case "quit" =>
    case "test" => getNodes("test") ! "test"
    case msg: String => {
      val request = msg.split(" ", 2)
      request(0) match {
        case "get" => {
          val node = getNodes(request(1))
          val future = node.ask(Get(request(1)))(5 seconds)
          try {
            val result = Await.result(future, timeout.duration)
            println(getResult(result))
          } catch {
            case timeout: TimeoutException => println("Await timeout")
          }
        }
        case "remove" => {
          val node = getNodes(request(1))
          val future = node.ask(Remove(request(1)))(5 seconds)
          try {
            val result = Await.result(future, timeout.duration)
            println(getResult(result))
          } catch {
            case timeout: TimeoutException => println("Await timeout")
          }
        }
        case "insert" => {
          val params = request(1).split("->", 2)
          val node = getNodes(params(0))
          val future = node.ask(Insert(params(0), params(1)))(5 seconds)
          try {
            val result = Await.result(future, timeout.duration)
            println(getResult(result))
          } catch {
            case timeout: TimeoutException => println("Await timeout")
          }
        }
        case "update" => {
          val params = request(1).split("->", 2)
          val node = getNodes(params(0))
          val future = node.ask(Update(params(0), params(1)))(5 seconds)
          try {
            val result = Await.result(future, timeout.duration)
            println(getResult(result))
          } catch {
            case timeout: TimeoutException => println("Await timeout")
          }
        }
        case _ => {
          println("Don't know command")
        }
      }
    }
    case _ =>
  }


  private def getNodes(key: String) = {
    nodes(key.hashCode % nodes.length)
  }

}

object Router {
  def props(nodes: Array[String]): Props = Props(classOf[Router], nodes)
}