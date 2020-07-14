package alertManager

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.types.StructType

object storgeAlert {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF);
    saveAlertIntoDfs("drone-alerting")
  }
  def saveAlertIntoDfs(topic:String): Unit ={
    val ss = SparkSession
      .builder()
      .master("local[*]")
      .appName("store_alert")
      .getOrCreate()

    import ss.implicits._

    val alertDf = ss
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("startingOffsets", "earliest")
      .option("maxOffsetsPerTrigger","6000")
      .option("subscribe", topic)
      .load()

    val alertJsonDf = alertDf.selectExpr("CAST(value AS STRING)").as[String]

    val schema = new StructType()
      .add("droneID","String")
      .add("location", "String")
      .add("time", "String")
      .add("violationCode", "String")
      .add("imageID","String")
      .add("image","String")

    val messageDF = alertJsonDf.select(from_json($"value", schema).as("message"))
    val alertFlattenDf = messageDF.selectExpr("message.droneID","message.location",
      "message.time", "message.violationCode","message.imageID","message.image")


    val fileSaver = alertFlattenDf.writeStream
      .outputMode("append")
      .format("parquet")
      .option("path","src/main/resources/output/droneAlert")
      .option("checkpointLocation", "src/main/resources/checkpointAlertFromDrone")
      .start()

    fileSaver.awaitTermination()
  }

}
