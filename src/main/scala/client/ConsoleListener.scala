package client

import Messages._
import akka.actor.{Props, Actor}
import akka.util.Timeout

/**
 * User: Vasily
 * Date: 03.10.13
 * Time: 14:36
 */
class ConsoleListener(nodes: Array[String]) extends Actor {
  val timeout = Timeout(5000)
  val sleepTime = 250
  val router = context.actorOf(Router.props(nodes), "route")
  var quit = 0

  def resultPre(id: Long) = "For request %d response is ".format(id)

  def getResult(result: Any): String = {
    result match {
      case Removed(done, id) => resultPre(id) + {
        if (done) "removed" else "some error"
      }
      case Answer(key, value, id) => resultPre(id) + "for " + key + " last record is " + value
      case OK(text, id) => resultPre(id) + text
      case Error(key, id) => resultPre(id) + "Error " + key
      case NoKey(key, id) => resultPre(id) + "No key found " + key
      case _ => "Unknown response"
    }
  }

  override def receive: Actor.Receive = {

    case null =>

    case GetClose() => {
      quit += 1
      if (quit == nodes.length) {
        context.system.shutdown()
      }
    }
    case "quit" => {
      router ! Close()
    }
    case ConsoleMessage(msg, id) => {
      val request = msg.split(" ", 2)
      request(0) match {
        case "get" => {
          router ! Get(request(1), id)
        }
        case "remove" => {
          router ! Remove(request(1), id)
        }
        case "insert" => {
          val params = request(1).split("->", 2)
          router ! Insert(params(0), params(1), id)
        }
        case "update" => {
          val params = request(1).split("->", 2)
          router ! Update(params(0), params(1), id)
        }
        case _ => {
          println("Don't know command")
        }
      }
    }
    case response: Response => println(getResult(response))
  }

}

object ConsoleListener {

  def props(nodes: Array[String]): Props = Props(classOf[ConsoleListener], nodes)

}