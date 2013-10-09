package client


/**
 * User: Vasily
 * Date: 01.10.13
 * Time: 21:52
 */
object Messages {

  abstract class Commands

  case class Get(key: String, id: Long) extends Commands {
  }

  case class Update(key: String, value: String, id: Long) extends Commands {
  }

  case class Insert(key: String, value: String, id: Long) extends Commands {
  }

  case class Remove(key: String, id: Long) extends Commands

  abstract class Response()

  case class Removed(done: Boolean, id: Long) extends Response

  case class Answer(key: String, value: String, id: Long) extends Response

  case class OK(text: String, id: Long) extends Response

  case class Error(key: String, id: Long) extends Response

  case class NoKey(key: String, id: Long) extends Response

  case class Close() extends Commands

  case class GetClose() extends Response

  case class ConsoleMessage(message: String, id: Long)

}