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

  /**
   * creates index of filename
   * @param filename should be correct db-file
   */


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
    val blockSize = 12
    val end = bytes.capacity() / blockSize //should divide to 4+8=12

    def binarySearch(left: Int, right: Int): Int = {
      if (right - left <= 1) {
        if (right == end) -1
        else if (bytes.getInt(right * blockSize) == hash) {
          right
        } else left
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
  }
}