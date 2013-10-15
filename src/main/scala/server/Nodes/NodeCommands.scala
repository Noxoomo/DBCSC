package server.Nodes

import server.OnDiskStorage.DiskStorageMaintains
import java.io.{File, RandomAccessFile}
import java.nio.MappedByteBuffer

/**
 * User: Vasily
 * Date: 15.10.13
 * Time: 8:30
 */
object NodeCommands {

  abstract class NodeMessages()

  case class Merge(n: Int, maintainer: DiskStorageMaintains);

  case class Merged(descriptors: (RandomAccessFile, MappedByteBuffer), old: (Array[File], Array[File]))

}
