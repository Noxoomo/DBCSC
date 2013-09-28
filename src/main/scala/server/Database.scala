package server


abstract class Database {
  /**
   * if key exists — return true, otherwise false
   * @param key
   * @return true if key exists
   */
  def contains(key: String): Boolean

  /**
   * if key exists throws   KeyExistsException, otherwise add key-value pair to storage
   * @param key
   * @param value
   * @throws KeyExistsException
   */
  def insert(key: String, value: String)

  /**
   * if key exists returns value of this key, otherwise throws NoKeyFoundException
   * @param key
   * @throws NoKeyFoundException
   *
   */
  def get(key: String): String

  /**
   * remove + insert
   * @param key
   * @param value
   * @throws KeyExistsException, NoKeyFoundException
   *
   */
  def update(key: String, value: String)


  /**
   *
   * @param key
   */
  def remove(key: String)
}
