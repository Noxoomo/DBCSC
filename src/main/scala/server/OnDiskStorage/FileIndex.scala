package server.OnDiskStorage

import java.io._
import Utils.FileUtils._
import scala.Long
import java.nio.MappedByteBuffer
import java.nio.channels.FileChannel
import java.nio.channels.FileChannel.MapMode

/**
 * User: Vasily
 * Date: 08.10.13
 * Time: 9:02
 */


object FileIndex {
  val intSize = 4

  class Entry(val hash: Int, val offset: Long)

  def sort(filename: String): MappedByteBuffer = {
    val workingDir = filename + ".indexSort/"
    val channel: FileChannel = new RandomAccessFile(workingDir + "index", "r").getChannel
    val all = channel.map(MapMode.READ_ONLY, 0L, channel.size())
    if (!pathExists(workingDir + "merge/")) createFolder(workingDir + "merge/")
    val sortedIndex = merge(all, 0, all.capacity() / 12, workingDir)
    if (pathExists(filename + ".index")) {
      removeFile(filename + ".index")
    }
    renameFile(sortedIndex, filename + ".index")
    removeFolder(workingDir + "merge")
    removeFolder(workingDir)
    val ind: FileChannel = new RandomAccessFile(filename + ".index", "r").getChannel
    ind.map(MapMode.READ_ONLY, 0L, ind.size())
  }

  private def merge(firstName: String, secondName: String, workingDir: String): String = {
    val firstChannel = new RandomAccessFile(firstName, "r").getChannel
    val first = firstChannel.map(MapMode.READ_ONLY, 0L, firstChannel.size())
    val secondChannel = new RandomAccessFile(secondName, "r").getChannel
    val second = secondChannel.map(MapMode.READ_ONLY, 0L, secondChannel.size())
    //first should be created before second

    if (pathExists(workingDir + "/merge/" + System.currentTimeMillis().toString)) {
      Thread.sleep(5)
    }
    val filename = workingDir + "/merge/" + System.currentTimeMillis().toString

    val writer = new DataOutputStream(new FileOutputStream(filename))


    while (first.position() < first.capacity() || second.position() < second.capacity()) {
      if (first.position() >= first.capacity() && second.position() < second.capacity()) {
        val index = second.getInt
        val offset = second.getLong
        writer.writeInt(index)
        writer.writeLong(offset)
      } else if (second.position() >= second.capacity() && first.position() < first.capacity()) {
        val index = first.getInt
        val offset = first.getLong
        writer.writeInt(index)
        writer.writeLong(offset)
      } else {
        val firstPos = first.position()
        val secondPos = second.position()
        val firstIndex = first.getInt
        val secondIndex = second.getInt
        if (firstIndex <= secondIndex) {
          val offset = first.getLong
          writer.writeInt(firstIndex)
          writer.writeLong(offset)
          second.position(secondPos)
        } else {
          val offset = second.getLong
          writer.writeInt(secondIndex)
          writer.writeLong(offset)
          first.position(firstPos)
        }
      }
    }
    writer.flush()
    writer.close()
    filename
  }

  def merge(index: MappedByteBuffer, start: Int, end: Int, path: String): String = {
    if ((end - start) <= 500000) {
      val entries = (for (i <- start to end - 1) yield
        new Entry(index.getInt(i * 12), index.getLong(i * 12 + 4))).sortBy(x => x.hash)
      if (pathExists(path + "merge/" + System.currentTimeMillis().toString)) {
        Thread.sleep(5)
      }
      val filename = path + "merge/" + System.currentTimeMillis().toString
      val writer = new DataOutputStream(new FileOutputStream(filename))
      for (entry <- entries) {
        writer.writeInt(entry.hash)
        writer.writeLong(entry.offset)
      }
      writer.flush()
      writer.close()
      filename
    } else {
      val mid: Int = (start + end) / 2
      val left = merge(index, start, mid, path)
      val right = merge(index, mid, end, path)
      val mergeFile = merge(left, right, path)
      removeFile(left)
      removeFile(right)
      mergeFile
    }
  }


  /**
   * creates index of filename
   * @param filename should be correct db-file
   */

  def indexBigFiles(filename: String): MappedByteBuffer = {
    if (!pathExists(filename)) throw new FileNotFoundException()
    val reader = new DataInputStream(new FileInputStream(filename))
    val indexSortDir = filename + ".indexSort/"
    createFolder(indexSortDir)
    val unsortedIndex = indexSortDir + "index"
    val writer = new DataOutputStream(new FileOutputStream(unsortedIndex))
    var offset = 0L
    var keyLength = reader.readInt()
    try {
      while (keyLength != -1) {
        val keyBytes = new Array[Byte](keyLength)
        val removed = reader.readBoolean()
        reader.read(keyBytes)
        val key = new String(keyBytes)
        writer.writeInt(key.hashCode) //int - 4 bytes
        writer.writeLong(offset) //long - 8 bytes
        offset += intSize + keyLength + 1
        offset += {
          if (!removed) {
            val valueLength = reader.readInt()
            reader.skipBytes(valueLength)
            valueLength + intSize
          } else 0
        }
        try {
          keyLength = reader.readInt()
        } catch {
          case e: EOFException => keyLength = -1
        }
      }
    } finally {
      reader.close()
      writer.flush()
      writer.close()
    }
    val res = sort(filename)
    removeFolder(indexSortDir + "merge/")
    removeFolder(indexSortDir)
    res
  }

  def index(filename: String): MappedByteBuffer = {
    if (!pathExists(filename)) throw new FileNotFoundException()
    var index = List[Entry]()
    val reader = new DataInputStream(new FileInputStream(filename))
    var offset = 0L
    var keyLength = reader.readInt()
    try {
      while (keyLength != -1) {
        val keyBytes = new Array[Byte](keyLength)
        val removed = reader.readBoolean()
        reader.read(keyBytes)
        val key = new String(keyBytes)
        index = new Entry(key.hashCode, offset) :: index
        offset += intSize + keyLength + 1
        offset += {
          if (!removed) {
            val valueLength = reader.readInt()
            reader.skipBytes(valueLength)
            valueLength + intSize
          } else 0
        }
        try {
          keyLength = reader.readInt()
        } catch {
          case e: EOFException => keyLength = -1
        }
      }
    } finally {
      reader.close()
    }
    val writer = new DataOutputStream(new FileOutputStream(filename + ".index"))
    for (entry <- index.toArray.sortBy(x => x.hash)) {
      writer.writeInt(entry.hash) //int - 4 bytes
      writer.writeLong(entry.offset) //long - 8 bytes
    }
    writer.flush()
    writer.close()
    val channel: FileChannel = new RandomAccessFile(filename + ".index", "r").getChannel
    channel.map(MapMode.READ_ONLY, 0L, channel.size())
  }

  def get(key: String, bytes: MappedByteBuffer): List[Long] = search(key.hashCode, bytes)

  private def search(hash: Int, bytes: MappedByteBuffer): List[Long] = {
    if (bytes != null) {
      val blockSize = 12
      val end = bytes.capacity() / blockSize //should divide to 4+8=12

      def binarySearch(left: Int, right: Int): Int = {
        if (right - left <= 1) {
          if (right == end) if (left == 0 && bytes.getInt(0) == hash) 0 else -1
          else if (bytes.getInt(left * blockSize) == hash) {
            left
          } else right
        } else {
          val mid: Int = (right + left) / 2
          val midHash = bytes.getInt(mid * blockSize)
          if (midHash < hash) {
            binarySearch(mid, right)
          } else {
            binarySearch(left, mid)
          }
        }
      }
      val start = binarySearch(0, end)

      def toList(start: Int): List[Long] = {
        if (bytes.capacity() > start * blockSize && bytes.getInt(start * blockSize) == hash) {
          bytes.getLong(blockSize * start + 4) :: toList(start + 1)
        } else List()
      }
      if (start != -1) toList(start) else List()
    } else List()
  }

}