package server.OnDiskStorage

import java.io._
import Utils.FileUtils._
import scala.{Long, Short}
import java.nio.MappedByteBuffer
import java.nio.channels.FileChannel
import java.nio.channels.FileChannel.MapMode

/**
 * User: Vasily
 * Date: 08.10.13
 * Time: 9:02
 */
class FileIndex() {
  private var indexes = List[MappedByteBuffer]()

  def addIndex(buffer: MappedByteBuffer) {
    indexes = buffer :: indexes
  }

  /**
   *
   * @param hash
   * @return list of offsets in file, where key.hash = hash
   */
  def get(hash: Short): List[Long] = {
    def recGet(list: List[MappedByteBuffer]) = {
      if (list.isEmpty) List()
      else {
        search(hash, list.head) ::: recGet(list.tail)
      }
    }
  }


  private def search(hash: Short, bytes: MappedByteBuffer): List[Long] = {
    val end = bytes.capacity() / 10 //should divide to 2+8=10

    def binarySearch(left: Int, right: Int): Int = {
      if (right - left <= 1) {
        if (right == end) -1
        else if (bytes.getShort(right * 10) == hash) {
          right
        } else -1
      } else {
        val mid: Int = (right + left) / 2
        val midHash = bytes.getShort(mid * 10)
        if (midHash < hash) {
          binarySearch(mid, right)
        } else {
          binarySearch(left, mid)
        }
      }
    }
    val start = binarySearch(0, end)
    def toList(pos: Int): List[Long] = {
      if (bytes.getShort(pos) == hash) {
        bytes.getLong(10 * pos + 2) :: toList(pos + 1)
      } else List()
    }
    if (start != -1) toList(start) else List()
  }
}

object FileIndex {
  val intSize = 4

  class Entry(val hash: Short, val offset: Long)

  /**
   * creates index of filename
   * @param filename
   */


  def index(filename: String): MappedByteBuffer = {
    if (!pathExists(filename)) throw new FileNotFoundException()
    var index = List[Entry]()
    val reader = new DataInputStream(new FileInputStream(filename))
    var offset = 0
    var keyLength = reader.readInt()
    while (keyLength != -1) {
      val keyBytes = new Array[Byte](keyLength)
      val key = new String(keyBytes)
      val removed = reader.readBoolean()
      reader.read(keyBytes)
      if (!removed) {
        val valueLength = reader.readInt()
        reader.skipBytes(valueLength)
      }
      index = (new Entry(key.hashCode.toShort, offset)) :: index
      offset += intSize + keyLength + 1
      offset += {
        if (!removed) {
          val valueLength = reader.readInt()
          reader.skipBytes(valueLength)
          valueLength + intSize
        } else 0
      }
      keyLength = reader.readInt()
    }
    val writer = new DataOutputStream(new FileOutputStream(filename + ".index"))
    for (entry <- index.toArray.sortBy(x => x.hash)) {
      writer.writeShort(entry.hash) //short - 2 bytes
      writer.writeLong(entry.offset) //long - 8 bytes
    }
    writer.flush()
    writer.close()
    val channel: FileChannel = new RandomAccessFile(filename + ".index", "r").getChannel();
    channel.map(MapMode.READ_ONLY, 0L, channel.size());
  }
}