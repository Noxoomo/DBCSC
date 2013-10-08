package server

import server.Exception.{DataBaseOpenException, NoKeyFoundException, KeyRemoveException, KeyExistsException}
import server.OnDiskStorage.DiskStorage
import server.OnDiskStorage.DiskStatus.{Value, NothingFound}

class Server(dbPath: String) {
  val db = new DiskStorage(dbPath)


  def processLine(line: String): String = {
    val request = line.split(" ", 2)
    try {
      request(0) match {
        case "quit" => {
          db.close()
          sys.exit(0)
        }
        case "insert" => {
          val query = request(1).split("->")
          if (query.length != 2) "Query syntax error"
          else {
            db.insert(query(0), query(1))
            "inserted"
          }
        }
        case "update" => {
          val query = request(1).split("->")
          if (query.length != 2) "Query syntax error"
          else {
            db.update(query(0), query(1))
            "updated"
          }
        }
        case "remove" => {
          db.remove(request(1))
          "removed"
        }
        case "get" => {
          val result = db.get(request(1))
          result match {
            case NothingFound() => "No key found"
            case Value(value) => value
          }
        }
        case _ => "unknown command"
      }
    } catch {
      case e: KeyExistsException => "Error, key already exists"
      case e: KeyRemoveException => "Error, database stopped"
      case e: NoKeyFoundException => "No key found"
      case e: DataBaseOpenException => "Can't open database"
    }
  }

}
