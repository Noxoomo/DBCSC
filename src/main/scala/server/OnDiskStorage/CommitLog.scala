package server.OnDiskStorage

/**
 * User: Vasily
 * Date: 28.09.13
 * Time: 21:01
 */
class CommitLog {
  def insert(key: String, value: String, pos: Long) = {
    write("Inserted " + " " + pos.toString + " " + key + " " + value)
  }

  def update(key: String, value: String, pos: Long) = {
    write("Updated " + key + " " + pos.toString + " " + value)
  }

  def remove(key: String, pos: Long) {
    write("removed " + " " + pos.toString + " " + key)
  }

  private def write(str: String) {
    //println(str)
  }

}
