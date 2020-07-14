package alertManager

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.types.StructType

object AlertHandler {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF);
    getAlertFromDrones("msg-2-stream", "drone-alerting")
  }

  def getAlertFromDrones(alertConsumerTopic: String, alertProducerTopic:String ): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("alert_consumer")
      .getOrCreate()

    import spark.implicits._

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("startingOffsets", "earliest")
      .option("maxOffsetsPerTrigger", "6000")
      .option("subscribe", alertConsumerTopic)
      .load()

    val messageJsonDf = df.selectExpr("CAST(value AS STRING)")
      .as[String]

    val schema = new StructType()
      .add("droneID","String")
      .add("location", "String")
      .add("time", "String")
      .add("violationCode", "String")
      .add("imageID","String")
      .add("image","String")


    val messageDF = messageJsonDf.select(from_json($"value", schema).as("message"))
    val messageFlattenDf = messageDF.selectExpr("message.droneID","message.location",
      "message.time", "message.violationCode","message.imageID","message.image")

    val alertFiler = messageFlattenDf.filter(messageFlattenDf("violationCode") === "00")
    val consoleOutPut = alertFiler.writeStream
      .outputMode("append")
      .format("console")
      .start()

    println("send alert to kafka topic")
    val alertDF = alertFiler.selectExpr( "to_json(struct(*)) AS value")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", alertProducerTopic)
      .option("checkpointLocation", "src/main/resources/checkpoint_alert")
      .start()

    alertDF.awaitTermination()
    consoleOutPut.awaitTermination()


  }
}
