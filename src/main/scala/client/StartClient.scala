package client

import akka.actor.ActorSystem
import scala.io.Source
import Utils.FileUtils._
import com.typesafe.config.ConfigFactory

/**
 * User: Vasily
 * Date: 01.10.13
 * Time: 20:34
 */
object StartClient {

  def loadNodes(path: String) = {
    (for (line <- Source.fromFile(path).getLines()) yield line).toArray
  }

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
    Iterator.continually(Console.readLine()).filter(_ != null).takeWhile(_ != "quit").foreach(client ! _)
    client ! "quit"
    system.awaitTermination()

  }

}
