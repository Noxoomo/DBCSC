import java.io.{FileWriter, BufferedWriter}
import scala.io.Source

/**
 * User: Vasily
 * Date: 29.09.13
 * Time: 22:46
 */

var i = 0L
val writer = new BufferedWriter(new FileWriter("stress-test"))
for (line <- Source.fromFile("data").getLines()) {
  val values = line.split(" ")
  for (value <- values) {
    writer.write("insert key" + i.toString + "->" + value + "\n")
    i += 1
  }
}
writer.flush()
writer.close()
