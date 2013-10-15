package server.Nodes

import akka.actor.{Props, Actor}
import server.Nodes.NodeCommands._
import server.OnDiskStorage.DiskStorage

/**
 * User: Vasily
 * Date: 15.10.13
 * Time: 8:29
 */
class Merger(val storage: DiskStorage) extends Actor {
  def receive: Actor.Receive = {
    case Merge(n) => {
      val (descriptors, toRemove) = storage.mergeLast(n)
      sender ! Merged(descriptors, toRemove)
    }
    case _ =>
  }
}

object Merger {
  def props(storage: DiskStorage): Props = Props(classOf[Merger], storage)
}


