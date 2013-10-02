package server

import server.Exception.{DataBaseOpenException, KeyReadException}


object SingleServer {


  def main(args: Array[String]) = {
    if (args.length == 0) println("Error, provide database path")
    else {
      val server = new Server(args(0))
      try {
        print("Starting database…")
        println("started")
        var finished = false
        while (!finished) {
          val line = Console.readLine()
          if (line == null || line.isEmpty) {
            finished = true
          } else {
            println(server.processLine(line))
          }
        }
      } catch {
        case e: KeyReadException => println("Can't recover commit log, exit")
        case e: DataBaseOpenException => println("Can't open data base")
      }
    }
  }

}
