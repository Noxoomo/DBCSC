package server.OnDiskStorage

/**
 * User: Vasily
 * Date: 08.10.13
 * Time: 23:19
 */
object DiskStatus {

  abstract class StorageResponse()

  case class Value(value: String) extends StorageResponse

  case class NothingFound() extends StorageResponse


  abstract class DiskLookupResult()

  case class FoundValue(value: String) extends DiskLookupResult

  case class WasRemoved() extends DiskLookupResult

  case class NoKeyFound() extends DiskLookupResult

}