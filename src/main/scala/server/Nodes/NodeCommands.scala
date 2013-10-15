package server.Nodes

import server.OnDiskStorage.DiskStorage
import java.io.{File, RandomAccessFile}
import java.nio.MappedByteBuffer

/**
 * User: Vasily
 * Date: 15.10.13
 * Time: 8:30
 */
object NodeCommands {

  abstract class NodeMessages()

  case class Merge(n: Int, storage: DiskStorage);

  case class Merged(descriptors: (RandomAccessFile, MappedByteBuffer), old: (Array[File], Array[File]))

}
