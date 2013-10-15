package server.Nodes

import akka.actor.Actor
import server.Nodes.NodeCommands._
import server.OnDiskStorage.DiskStorage

/**
 * User: Vasily
 * Date: 15.10.13
 * Time: 8:29
 */
class Merger extends Actor {
  def receive: Actor.Receive = {
    case Merge(n, storage: DiskStorage) => {
      val (descriptors, toRemove) = storage.mergeLast(n)
      sender ! Merged(descriptors, toRemove)
    }
  }
}


