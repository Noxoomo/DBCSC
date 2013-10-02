package server.Traits

trait Database {
  /**
   * if key exists — return true, otherwise false
   * @param key for check
   * @return true if key exists
   */
  def contains(key: String): Boolean

  /**
   * add key-value pair to storage
   * @param key  for insert
   * @param value for insert
   * @throws KeyExistsException  if key exists
   */
  def insert(key: String, value: String)

  /**
   * if key exists returns value of this key, otherwise throws NoKeyFoundException
   * @param key finds key in db and returns value
   * @throws NoKeyFoundException    if key doesn't exist
   *
   */
  def get(key: String): String

  /**
   * remove + insert
   * @param key  to update
   * @param value new value
   * @throws KeyExistsException, NoKeyFoundException if KeyExists or no key in DB
   *
   */
  def update(key: String, value: String)


  /**
   *
   * @param key   to remove
   */
  def remove(key: String)
}
