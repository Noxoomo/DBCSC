package server.Nodes

import akka.actor.{Props, Actor}
import client.Messages._
import scala.util.Random
import server.OnDiskStorage.DiskStorage
import server.OnDiskStorage.DiskStatus.{NothingFound, Value}
import server.Nodes.NodeCommands.{Merge, Merged, NodeMessages}


/**
 * User: Vasily
 * Date: 01.10.13
 * Time: 8:38
 */
//node name = path to node working filename
class Node(private val nodeName: String) extends Actor {
  private val dbPath = if (nodeName.endsWith("/")) nodeName else nodeName + "/"
  private val storage = new DiskStorage(dbPath)
  private val rand = new Random()
  private val merger = context.actorOf(Merger.props(storage))
  private var collecting: Boolean = false
  private val maxFiles = 2

  override def receive = {
    case "ping" => sender ! "ping"
    case query: Commands => {
      if (storage.count > maxFiles) {
        if (!collecting) {
          collecting = true
          merger ! Merge(storage.count)
        }

      }
      query match {
        case Get(key, id) => {
          val storageResponse = storage.get(key)
          storageResponse match {
            case Value(value) => sender ! Answer(key, value, id)
            case NothingFound() => sender ! NoKey(key, id)
          }
        }

        case Remove(key, id) => {
          storage.remove(key)
          sender ! Removed(done = true, id)
        }
        case Insert(key: String, value: String, id) => {
          storage.insert(key, value)
          sender ! OK("key inserted", id)
        }
        case Update(key: String, value: String, id) => {
          storage.insert(key, value)
          sender ! OK("Key updated", id)
        }
        case Close() => {
          sender ! GetClose()
        }
        case GC() => {
          collecting = true
          merger ! Merge(storage.count)
        }
        case _ => sender ! "unknown command"
      }
    }
    case info: NodeMessages => {
      info match {
        case Merged(descriptors, old) => {
          storage.replaceLast(descriptors, old)
          collecting = false
        }
      }
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