package server.OnDiskStorage

import java.io.{FileWriter, BufferedWriter}

/**
 * User: Vasily
 * Date: 28.09.13
 * Time: 21:01
 */
class CommitLog {
  val writer = new BufferedWriter(new FileWriter("commits"))

  //val writer = new BufferedWriter(new PrintWriter(System.err))
  def insert(key: String, value: String, pos: Long) = {
    write("inserted " + " " + pos.toString + " " + key + " " + value)
  }

  def update(key: String, value: String, pos: Long) = {
    write("updated " + key + " " + pos.toString + " " + value)
  }

  def remove(key: String, pos: Long) {
    write("removed " + " " + pos.toString + " " + key)
  }

  private def write(str: String) {
    val log = " " + str
    writer.write(log.length + log)
    writer.flush()
  }

  def close() {
    writer.close()
    //removeFile("commits")
  }

}
