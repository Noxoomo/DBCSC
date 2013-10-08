package client

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.ask
import scala.concurrent.duration._
import scala.concurrent.{TimeoutException, Await}
import akka.util.Timeout
import Utils.FileUtils._
import client.Messages.ConsoleMessage

/**
 * User: Vasily
 * Date: 06.10.13
 * Time: 19:31
 */

object StartDatabase {
  val timeout = new Timeout(10000)

  def main(args: Array[String]) {
    if (args.length == 0 || !pathExists(args(0))) {
      println("error, database path")
      System.exit(0)
    }
    val system = ActorSystem("Database")
    val database = system.actorOf(server.Nodes.Node.props(args(0)))
    val nodes = Array(database.path.toString)
    val client = system.actorOf(ConsoleListener.props(nodes), "Console")
    Iterator.continually(Console.readLine()).filter(_ != null).takeWhile(_ != "quit")
      .foreach(client ! ConsoleMessage(_, System.currentTimeMillis()))
    client ! "quit"
    system.awaitTermination()
  }


  def send(str: String, client: ActorRef) {
    try {
      Await.result(client.ask(str)(5 seconds), timeout.duration)
    } catch {
      case e: TimeoutException =>
    }
  }
}

