package server.Nodes

import server.Storage
import akka.actor.{Props, Actor}
import client.Messages._


/**
 * User: Vasily
 * Date: 01.10.13
 * Time: 8:38
 */
//node name = path to node working dir
class Node(private val nodeName: String) extends Actor {
  private val dbPath = if (nodeName.endsWith("/")) nodeName else nodeName + "/"
  private val storage = new Storage(dbPath)


  def receive = {

    case Get(key) => {
      val response = if (storage contains key) Answer(key, storage.get(key))
      else NoKey(key)
      sender ! response
    }
    case "test" => {
      sender ! "test 1"
    }
    case Remove(key) => {
      if (storage contains key) {
        storage.remove(key)
        sender ! Removed(true)
      }
      else sender ! Removed(false)
    }
    case Insert(key: String, value: String) => {
      if (storage contains (key)) {
        println("key already exists")
        sender ! Error("Key already exists")
      } else {
        println("insert %s %s".format(key, value))
        storage.insert(key, value)
        sender ! OK("key inserted")
      }
    }
    case Update(key: String, value: String) => {
      if (!storage.contains(key)) {
        sender ! NoKey(key)
      } else {
        storage.update(key, value)
        sender ! OK("Key updated")
      }
    }
    case _ => print("some message")
  }


}

object Node {
  def props(nodeName: String): Props = Props(classOf[Node], nodeName)
}