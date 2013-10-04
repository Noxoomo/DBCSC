package server.OnDiskStorage

import java.io.{FileWriter, BufferedWriter}
import Utils.FileUtils._

/**
Commit log — if db crashed during write => restore from commit log
  commit log stores only insert/update operations: it place where in db should be key-value
  */
class CommitLog(dir: String) {
  val writer = new BufferedWriter(new FileWriter(dir + CommitLog.fileName))
  val remove = new BufferedWriter(new FileWriter(dir + CommitLog.fileName + ".remove"))

  def write(key: String, value: String, index: Long) {
    writer.write(("%s\n%d %d %d\n%s").format(key, index, key.length, value.length, value))
    writer.flush()
  }

  def remove(index: Long) {
    remove.write(index + "\n")
    remove.flush()
  }

  def close() {
    writer.close()
    remove.close()
    removeFile(dir + CommitLog.fileName)
    removeFile(dir + CommitLog.fileName + ".remove")
  }

}

object CommitLog {
  def remove(dir: String) {
    removeFile(dir + fileName)
    removeFile(dir + fileName + ".remove")
  }

  val fileName = "commits"
}