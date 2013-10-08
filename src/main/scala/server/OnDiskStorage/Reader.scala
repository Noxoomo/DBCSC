package server.OnDiskStorage

import java.io.{DataOutputStream, RandomAccessFile}

/**
 * User: Vasily
 * Date: 08.10.13
 * Time: 13:29
 */
class Reader(file: RandomAccessFile) {
  var pos = 0
  var notEmpty = true

  def hasNext(): Boolean = {
    if (file.getFilePointer != file.length()) true else false
  }

  private val entry: Entry = new Entry()

  def next() = {
    if (!hasNext()) notEmpty = false
    else {
      val keyLen = file.readInt()
      entry.timestamp = file.readLong()
      entry.removed = file.readBoolean()
      val keyBytes = new Array[Byte](keyLen)
      file.read(keyBytes)
      entry.key = new String(keyBytes)

      if (!entry.removed) {
        val valueLen = file.readInt()
        val valueBytes = new Array[Byte](valueLen)
        file.read(valueBytes)
        entry.value = new String(valueBytes)
      }
    }
  }

  def current() = entry

  def write(writer: DataOutputStream) {
    if (!entry.removed) {
      val keyBytes = entry.key.getBytes()
      writer.writeInt(keyBytes.length)
      writer.writeLong(entry.timestamp)
      writer.writeBoolean(false)
      writer.write(keyBytes)
      val valueBytes = entry.value.getBytes()
      writer.writeInt(valueBytes.length)
      writer.write(valueBytes)
    }

  }


}

class Entry() {
  var key: String = ""
  var value: String = ""
  var timestamp: Long = 0
  var removed: Boolean = true

}
