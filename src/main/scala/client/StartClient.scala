package client

import akka.actor._
import scala.io.Source
import Utils.FileUtils._
import com.typesafe.config.ConfigFactory
import akka.pattern.ask
import scala.concurrent.duration._
import scala.concurrent.{TimeoutException, Await}
import akka.util.Timeout

/**
 * User: Vasily
 * Date: 01.10.13
 * Time: 20:34
 */
object StartClient {

  def loadNodes(path: String) = {
    (for (line <- Source.fromFile(path).getLines()) yield line).toArray
  }

  val timeout = new Timeout(5000)

  def main(args: Array[String]) {

    if (!pathExists(args(0)) || !pathExists(args(1))) {
      println("No config file found")
      System.exit(0)
    }
    val nodes = loadNodes(args(0))
    val akkaConf = System.getProperty("user.dir") + "/" + args(1)
    val conf = Source.fromFile(akkaConf).mkString
    val customConf = ConfigFactory.parseString(conf)
    val system = ActorSystem("Client", ConfigFactory.load(customConf)) //,ConfigFactory.load(akkaConfig))
    val client = system.actorOf(ConsoleListener.props(nodes), "Console")
    Iterator.continually(Console.readLine()).filter(_ != null).takeWhile(_ != "quit")
      .foreach(send(_, client))
    val future = client.ask("quit")(5 seconds)
    try {
      Await.result(future, 5 seconds)
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
