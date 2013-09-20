package server

import server.Exception.{DataBaseOpenException, NoKeyFoundException, DatabaseNotStartedException, KeyExistsException}

class Server(filename: String) {
  val db = new Database(filename)

  def startDatabase() {
    db.start()
  }

  def stop() {
    db.stop()
  }

  def processLine(line: String): String = {
    val request = line.split(" ", 2)
    try {
      request(0) match {
        case "quit" => {
          db.stop()
          sys.exit(0)
        }
        case "flush" => {
          db.flush()
          "flush done"
        }
        case "insert" => {
          val query = request(1).split("->")
          if (query.length != 2) "Query syntax error"
          else {
            db.add(query(0), query(1))
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
        case "get" => db.get(request(1))
        case _ => "unknown command"
      }
    } catch {
      case e: KeyExistsException => "Error, key already exists"
      case e: DatabaseNotStartedException => "Error, database stopped"
      case e: NoKeyFoundException => "No key found"
      case e: DataBaseOpenException => "Can't open database"
    }
  }

}
