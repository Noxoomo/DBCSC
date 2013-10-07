package Utils

import java.nio.file.{Paths, Files}
import java.io.File

/**
 * User: Vasily
 * Date: 28.09.13
 * Time: 20:42
 */
object FileUtils {
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

  def removeFile(filename: String) {
    new File(filename).delete()
  }

  def touch(filename: String) {
    if (!pathExists(filename))
      new File(filename).createNewFile()
  }

  def pathExists(filename: String): Boolean = {
    Files.exists(Paths.get(filename))
  }

  def createFolder(folderName: String) = {
    Files.createDirectories(Paths.get(folderName))
  }

  def renameFile(from: String, to: String) {
    new File(from).renameTo(new File(to))
  }


  def copyFile(from: String, to: String) {
    Files.copy(Paths.get(from), Paths.get(to))
  }
}
