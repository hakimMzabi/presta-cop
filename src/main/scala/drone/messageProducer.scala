package csvStreamer

import java.util.Properties

import drone.droneMessage
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.log4j.{Level, Logger}
import play.api.libs.json.{Json, OWrites}

import scala.io.Source

object messageProducer {
  implicit val residentWrites: OWrites[droneMessage] = Json.writes[droneMessage]

  val pathToFile = "./data/drones_messages.csv"
  val topic = "msg-2-stream"
  println("drone simulator started..")

  val props: Properties = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])

  val msgProducer: KafkaProducer[String, String] = new KafkaProducer[String, String](props)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF);
    extractDroneMsg(pathToFile)
  }

  def extractDroneMsg(pathToFile: String): Unit = {

    println("extracting lines from messages..")
    val bufferedSource = Source.fromFile(pathToFile)
    val header = bufferedSource.getLines.next().split(",").toList
    println(header)
    bufferedSource
      .getLines()
      .drop(1)
      .foreach(line => extractMsgLines(header, line.split(",").toList))

    bufferedSource.close()
  }

  def extractMsgLines(header: List[String], lineList: List[String]): Any = lineList match {
    //todo must fix outOfRange issue
    case x  => {
      val droneID = x(header.indexOf("id"))
      val location = x(header.indexOf("address"))
      val time = x(header.indexOf("date"))
      val violationCode = x(header.indexOf("violationCode"))
      val imageID = x(header.indexOf("imageId"))
      val image = x(header.indexOf("imageCode"))
      val data = Json.toJson(droneMessage(droneID, location, time, violationCode, imageID, image)).toString()
      val record = new ProducerRecord[String, String](topic, "key", data)
      msgProducer.send(record)
      println("sending stream into topic..")
    }
    case _ => println("no more data!..")
  }

}