package server.OnDiskStorage

import java.io.{FileWriter, BufferedWriter}
import Utils.FileUtils._

/**
Commit log — if db crashed during write => restore from commit log
  commit log stores only insert/update operations: it place where in db should be key-value
  */
class CommitLog(dir: String) {
  val writer = new BufferedWriter(new FileWriter(dir + CommitLog.fileName, true))


  def insert(key: String, value: String) {
    writer.write(("%s\n%d\n%s\n").format(key, value.length, value))
    writer.flush()
  }

  def remove(key: String) {
    writer.write("%s->\n".format(key))
    writer.flush()
  }


  def close() {
    writer.close()
    CommitLog.removeCommitLog(dir)
  }

}

object CommitLog {
  def removeCommitLog(dir: String) {
    removeFile(dir + fileName)
  }


  val fileName = "commits"
}