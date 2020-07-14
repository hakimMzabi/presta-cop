package csvStreamer

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType


object messageConsumer {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF);
    consumeFromKafka("msg-2-stream")
  }

  def consumeFromKafka(topic: String): Unit = {

    println("message consumer started..")

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
      .option("subscribe", topic)
      .load()

    val messageJsonDf = df.selectExpr("CAST(value AS STRING)")
      .as[String]

//message schema
    val schema = new StructType()
      .add("droneID", "String")
      .add("location", "String")
      .add("time", "String")
      .add("violationCode", "String")
      .add("imageID", "String")
      .add("image", "String")

    val messageDF = messageJsonDf.select(from_json($"value", schema).as("message"))
    val messageFlattenDf = messageDF
      .selectExpr("message.droneID", "message.location", "message.time",
        "message.violationCode", "message.imageID","message.image")

//write stream batch on the console
    val consoleOutPut = messageFlattenDf.writeStream
      .outputMode("append")
      .format("console")
      .start()

//save Stream into parquet partion grouped by violation code

   val fileSaver = messageFlattenDf.writeStream
      .outputMode("append")
      .format("parquet")
      .partitionBy("violationCode")
      .option("path","src/main/resources/output/")//todo hadoop path
      .option("checkpointLocation", "src/main/resources/chkpoint")
      .start()


    fileSaver.awaitTermination()
    consoleOutPut.awaitTermination()


  }
}



