package server.OnDiskStorage

import scala.io.Source
import java.io.{BufferedWriter, FileWriter}
import server.Exception.{LoadIndexException, NoKeyFoundException}
import server.Utils.FileUtils._
import scala.collection.mutable


/**
 * User: Vasily
 * Date: 28.09.13
 * Time: 20:49
 */


class KeyIndex(indexFilename: String, indexLock: String) {
  val index = new mutable.HashMap[String, Long]()
  if (!pathExists(indexFilename)) touch(indexFilename)
  //load index
  try {

    for (line <- Source.fromFile(indexFilename).getLines()) {
      val keyPos = line.split(" ")
      index.put(keyPos(0), keyPos(1).toLong)
    }
  } catch {
    case _: Throwable => throw new LoadIndexException()
  }

  val writer = new BufferedWriter(new FileWriter(indexFilename, true))

  def contains(key: String) = index.contains(key)

  def insert(key: String, position: Long) {
    writer.append(key + " " + position + "\n")
    writer.flush()
    index.put(key, position)
  }

  def remove(key: String) = index.remove(key)

  def get(key: String): Long = {
    val answer = index.get(key)
    if (answer.isDefined) answer.get
    else throw new NoKeyFoundException
  }

  def lock() {
    touch(indexLock)
  }

  def close() {
    writer.close()
    removeFile(indexLock)
  }


}
