package csvStreamer

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.log4j.{Level, Logger}
import play.api.libs.json.{Json, OWrites}
import scala.io.Source

object csvProducer {
  implicit val residentWrites: OWrites[csvLine] = Json.writes[csvLine]

  val pathToFile = "./data/Parking_Violations.csv"
  val topic = "csv-2-stream"
  println("csv streamer started..")

  val props: Properties = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])

  val csvProducer: KafkaProducer[String, String] = new KafkaProducer[String, String](props)
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF);
    extractCSV(pathToFile)
  }

  def extractCSV(pathToFile: String): Unit = {

    println("extracting lines from csv..")
    val bufferedSource = Source.fromFile(pathToFile)
    val header = bufferedSource.getLines.next().split(",").toList

    bufferedSource
      .getLines()
      .drop(1)
      .foreach(line => extractLines(header, line.split(",").toList))

    bufferedSource.close()
  }

  def extractLines(header: List[String], lineList: List[String]): Any = lineList match {

    case x if lineList.size > header.indexOf("Street Name") => {
      val location = x(header.indexOf("House Number")) + " " + x(header.indexOf("Street Name"))
      val time = x(header.indexOf("Issue Date"))
      val violationCode = x(header.indexOf("Violation Code"))
      val data = Json.toJson(csvLine(None, location, time, Some(violationCode))).toString()
      val record = new ProducerRecord[String, String](topic, "key", data)
      csvProducer.send(record)
      println("sending stream into topic..")
    }
    case _ =>
  }

}

