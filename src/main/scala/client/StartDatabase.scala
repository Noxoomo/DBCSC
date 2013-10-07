package client

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.ask
import scala.concurrent.duration._
import scala.concurrent.{TimeoutException, Await}
import akka.util.Timeout

/**
 * User: Vasily
 * Date: 06.10.13
 * Time: 19:31
 */

object StartDatabase {
  val timeout = new Timeout(10000)

  def main(args: Array[String]) {
    val system = ActorSystem("Database")
    val database = system.actorOf(server.Nodes.Node.props(args(0)))
    val nodes = Array(database.path.toString)
    val client = system.actorOf(ConsoleListener.props(nodes), "Console")
    Iterator.continually(Console.readLine()).filter(_ != null).takeWhile(_ != "quit")
      .foreach(send(_, client))
    val future = client.ask("quit")(10 seconds)
    try {
      Await.result(future, 10 seconds)
    } catch {
      case timeout: TimeoutException => println("shutdown timeout")
    }
    system.shutdown()
  }


  def send(str: String, client: ActorRef) {
    try {
      Await.result(client.ask(str)(5 seconds), timeout.duration)
    } catch {
      case e: TimeoutException =>
    }
  }
}

