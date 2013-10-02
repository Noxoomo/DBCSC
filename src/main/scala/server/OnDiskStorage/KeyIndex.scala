package server.OnDiskStorage

import scala.io.Source
import java.io.{RandomAccessFile, BufferedWriter, FileWriter}
import server.Exception.{ReindexException, LoadIndexException, NoKeyFoundException}
import Utils.FileUtils._
import scala.collection.mutable


/**
 * User: Vasily
 * Date: 28.09.13
 * Time: 20:49
 */


class KeyIndex(indexFilename: String, database: String, indexLock: String) {
  val index = new mutable.HashMap[String, Long]()
  if (!pathExists(indexFilename)) touch(indexFilename)
  var writer: BufferedWriter = null
  if (pathExists(indexLock)) reIndex() else loadIndex()

  //load index
  private def loadIndex() {
    try {

      for (line <- Source.fromFile(indexFilename).getLines()) {
        val keyPos = line.split(" ")
        index.put(keyPos(0), keyPos(1).toLong)
      }
    } catch {
      case _: Throwable => throw new LoadIndexException()
    }
    writer = new BufferedWriter(new FileWriter(indexFilename, true))
  }


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

  /**
   * reindex database
   */
  def reIndex() {
    val dbFile = new RandomAccessFile(database, "r")
    index.clear()
    writer = new BufferedWriter(new FileWriter(indexFilename))
    var bytesRead = 0L
    try {
      while (dbFile.getFilePointer < dbFile.length()) {
        val pos = dbFile.getFilePointer
        val keySize = dbFile.readInt()
        val valueSize = dbFile.readInt()
        val removed = dbFile.readBoolean()
        if (!removed) {
          val bytes = new Array[Byte](keySize)
          bytesRead += dbFile.read(bytes)
          val block = new String(bytes)
          insert(block, pos)
          dbFile.seek(dbFile.getFilePointer + valueSize)
        } else dbFile.seek(dbFile.getFilePointer + keySize + valueSize)
      }
    } catch {
      case e: Throwable => throw new ReindexException()
    } finally {
      dbFile.close()
    }
  }
}
