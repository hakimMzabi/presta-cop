package alertManager

import java.time.Duration
import javax.mail.internet.{InternetAddress, MimeMessage}
import javax.mail.{Message, MessagingException, Session}
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.json4s.jackson.JsonMethods._
import scala.collection.JavaConverters._



object alertNotificator {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF);
    getAlertsFromDFS()
  }

  def jsonStrToMap(jsonStr: String): Map[String, Any] = {
    implicit val formats = org.json4s.DefaultFormats
    parse(jsonStr).extract[Map[String, Any]]
  }

  def getAlertsFromDFS(): Unit = {
    val TOPIC = "drone-alerting"
    import java.util.Properties

    val sparkConf = new SparkConf()
      .setAppName("notify")
      .setMaster("local[*]")

    val sparkSession = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

    val host = "smtp.gmail.com"
    val port = "465"
    val subject = "Drone Alerting"
    val toAddress = "*******@gmail.com"
    val userName = "droneAlertGes@gmail.com"
    val password = "*****"

    val mailProperties = new Properties()
    mailProperties.put("mail.stmp.auth", "true")
    mailProperties.put("mail.stmp.starttls.enable", "true")
    mailProperties.put("mail.smtp.port", port);
    mailProperties.put("mail.debug", "true");
    mailProperties.put("mail.smtp.socketFactory.port", port);
    mailProperties.put("mail.smtp.socketFactory.class", "javax.net.ssl.SSLSocketFactory");
    mailProperties.put("mail.smtp.socketFactory.fallback", "false");

    val kafkaProperties = new Properties()
    kafkaProperties.put("bootstrap.servers", "localhost:9092")
    kafkaProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    kafkaProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    kafkaProperties.put("group.id", "consumergroup")

    val consumer:KafkaConsumer[String, String] = new KafkaConsumer[String, String](kafkaProperties)
    consumer.subscribe(List(TOPIC).asJava)

    while (true) {
      println("loading data from topic..")
      val records:ConsumerRecords[String, String] = consumer.poll(Duration.ofMillis(100))
      records.asScala.foreach { r =>
        val df = jsonStrToMap(r.value())
        println("extracting alert from stream..")
        println(df)

        val session = Session.getDefaultInstance(mailProperties, null)
        val message = new MimeMessage(session)

        message.addRecipient(Message.RecipientType.TO, new InternetAddress(toAddress));
        message.setSubject(subject)
        message.setContent("<html><body>" +
          "<h1>An alert was detected from " + df("droneID") +
          ": </h1> <br>" +
          "<h3>Violation code : " + df("violationCode") + "<br>" +
          "Violation Time : " + df("time") + "<br>" +
          "Location : " + df("location") + "<br></h3><br>" +
          "</body></html>", "text/html")

        val transport = session.getTransport("smtp")
        try {
          transport.connect(host, userName, password)
          transport.sendMessage(message, message.getAllRecipients)
          println("message delivered")
        } catch {
          case e: MessagingException => println(e)

        }
        transport.close()
      }
    }
  }
}




