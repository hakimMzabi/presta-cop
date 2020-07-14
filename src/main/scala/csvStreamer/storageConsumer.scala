package csvStreamer

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType


object storageConsumer {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF);
    val msgTopic = "msg-2-stream"
    val csvTopic = "csv-2-stream"

    consumerFromKafka(csvTopic, msgTopic)
  }

  def consumerFromKafka(csvTopic: String, msgTopic:String): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("csv_consumer")
      .getOrCreate()

    import spark.implicits._

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("startingOffsets", "earliest")
      .option("maxOffsetsPerTrigger","6000")
      .option("subscribe", csvTopic+","+msgTopic)
      .load()

    val storageJsonDf = df.selectExpr("CAST(value AS STRING)").as[String]

    val schema = new StructType()
      .add("droneID","String")
      .add("location", "String")
      .add("time", "String")
      .add("violationCode", "String")
      .add("imageID","String")
      .add("image","String")



    val messageDF = storageJsonDf.select(from_json($"value", schema).as("message"))
    val messageFlattenDf = messageDF.selectExpr("message.droneID","message.location",
      "message.time", "message.violationCode","message.imageID","message.image")

    val consoleOutPut = messageFlattenDf.writeStream
      .outputMode("append")
      .format("console")
      .start()


    val fileSaver = messageFlattenDf.writeStream
      .outputMode("append")
      .format("parquet")
      //.partitionBy("violationCode")
      .option("path","src/main/resources/output/")
      .option("checkpointLocation", "src/main/resources/chkpoint")
      .start()

    fileSaver.awaitTermination()
    consoleOutPut.awaitTermination()

  }
}


