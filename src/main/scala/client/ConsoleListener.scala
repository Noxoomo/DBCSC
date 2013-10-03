package client

import Messages._
import akka.actor.{Props, Actor}
import akka.pattern.ask
import scala.concurrent.duration._
import scala.concurrent.{TimeoutException, Await}
import akka.util.Timeout

/**
 * User: Vasily
 * Date: 03.10.13
 * Time: 14:36
 */
class ConsoleListener(nodes: Array[String]) extends Actor {
  val router = context.actorOf(Router.props(nodes), "route")
  val timeout = Timeout(1000)

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
    case msg: String => {
      val request = msg.split(" ", 2)
      request(0) match {
        case "get" => {
          val future = router.ask(Get(request(1)))(5 seconds)
          try {
            val result = Await.result(future, timeout.duration)
            println(getResult(result))
          } catch {
            case timeout: TimeoutException => println("Await timeout")
          }
        }
        case "remove" => {
          val future = router.ask(Remove(request(1)))(5 seconds)
          try {
            val result = Await.result(future, timeout.duration)
            println(getResult(result))
          } catch {
            case timeout: TimeoutException => println("Await timeout")
          }
        }
        case "insert" => {
          val params = request(1).split("->", 2)
          val future = router.ask(Insert(params(0), params(1)))(5 seconds)
          try {
            val result = Await.result(future, timeout.duration)
            println(getResult(result))
          } catch {
            case timeout: TimeoutException => println("Await timeout")
          }
        }
        case "update" => {
          val params = request(1).split("->", 2)
          val future = router.ask(Update(params(0), params(1)))(5 seconds)
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


}

object ConsoleListener {

  def props(nodes: Array[String]): Props = Props(classOf[Router], nodes)

}