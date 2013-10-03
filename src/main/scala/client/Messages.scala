package client


/**
 * User: Vasily
 * Date: 01.10.13
 * Time: 21:52
 */
object Messages {

  abstract class Commands

  case class Get(key: String) extends Commands {
  }

  case class Update(key: String, value: String) extends Commands {
  }

  case class Insert(key: String, value: String) extends Commands {
  }

  case class Remove(key: String) extends Commands {
  }

  abstract class Response

  case class Removed(done: Boolean) extends Response

  case class Answer(key: String, value: String) extends Response

  case class OK(text: String) extends Response

  case class Error(key: String) extends Response

  case class NoKey(key: String) extends Response

}