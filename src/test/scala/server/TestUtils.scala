package server

import java.io.File
import java.nio.file.{Paths, Files}

/**
 * User: Vasily
 * Date: 28.09.13
 * Time: 19:18
 */
object TestUtils {
  def clean(filename: String) {
    val commit = new File(filename + ".commit")
    if (commit.exists()) commit.delete()
    val db = new File(filename)
    if (db.exists()) db.delete()
  }

  def removeFolder(path: String): Boolean = {
    if (Files.exists(Paths.get(path))) {
      val dbRoot = new File(path)
      for (file <- dbRoot.listFiles()) {
        file.delete()
      }
      dbRoot.delete()
    }
    true
  }

}
