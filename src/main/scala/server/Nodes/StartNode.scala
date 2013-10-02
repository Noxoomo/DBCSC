package server.Nodes

import Utils.FileUtils._
import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import scala.io.Source


/**
 * User: Vasily
 * Date: 02.10.13
 * Time: 17:53
 */
object StartNode extends App {
  if (args.length == 0) {
    println("Provide node working dir")
    System.exit(0)
  }
  if (!pathExists(args(0))) {
    println("Can't find working dir")
    System.exit(0)
  }


  val nodeName = if (args(0).endsWith("/")) args(0) else args(0) + "/"
  val akkaConf = System.getProperty("user.dir") + "/" + nodeName + "akka"
  val customConf = ConfigFactory.parseString(Source.fromFile(akkaConf).mkString)
  val system = ActorSystem("Node", ConfigFactory.load(customConf))

  val node = system.actorOf(Node.props(nodeName), "Storage")
  Iterator.continually(Console.readLine()).filter(_ != null).takeWhile(_ != "quit")
  system.awaitTermination()

}
