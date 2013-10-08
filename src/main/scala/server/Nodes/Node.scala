package server.Nodes

import server.Storage
import akka.actor.{Props, Actor}
import client.Messages._
import scala.util.Random


/**
 * User: Vasily
 * Date: 01.10.13
 * Time: 8:38
 */
//node name = path to node working dir
class Node(private val nodeName: String) extends Actor {
  private val dbPath = if (nodeName.endsWith("/")) nodeName else nodeName + "/"
  private val storage = new Storage(dbPath)
  private val rand = new Random()


  def receive = {
    case Get(key, id) => {
      val response = if (storage contains key) Answer(key, storage.get(key), id)
      else NoKey(key, id)
      sender ! response
    }

    case Remove(key, id) => {
      if (storage contains key) {
        storage.remove(key)
        sender ! Removed(done = true, id)
      }
      else sender ! Removed(done = false, id)
    }
    case Insert(key: String, value: String, id) => {
      if (storage contains key) {
        sender ! Error("Key already exists", id)
      } else {
        storage.insert(key, value)
        sender ! OK("key inserted", id)
      }
    }
    case Update(key: String, value: String, id) => {
      if (!storage.contains(key)) {
        sender ! NoKey(key, id)
      } else {
        storage.update(key, value)
        sender ! OK("Key updated", id)
      }
    }
    case Close() => {
      sender ! OK("Got message", System.currentTimeMillis())
    }
    case _ => sender ! Error("unknown command", -1)
  }

  override def postStop() {
    storage.close()
  }


}

object Node {
  def props(nodeName: String): Props = Props(classOf[Node], nodeName)
}