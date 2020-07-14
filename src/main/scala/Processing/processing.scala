package Processing

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import UtilsFun.processUtils._

object processing {
  val pathToFile = "src/main/resources/output/"

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF);
    processFromDFS(pathToFile: String)
  }

  def processFromDFS(pathToFile:String): Unit = {

    println("Processing job started..")
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("process_consumer")
      .getOrCreate()

    val generalDf = readFromDFS(pathToFile, "parquet", None)(spark)
    generalDf.show()

    val top10Violations = generalDf.groupBy("violationCode")
      .agg(count("violationCode").alias("number_of_violations"))
      .sort(desc("number_of_violations"))
      .limit(10)
    writeIntoDFS(top10Violations, "csv","src/main/resources/viz/top10Viol")(spark)
    top10Violations.show()

    val ViloationPerDate = generalDf.groupBy("time")
      .agg(count("violationCode").alias("violations_per_date"))
    writeIntoDFS(ViloationPerDate, "csv","src/main/resources/viz/violPerDate")(spark)

    val top10ViloationPerDate = generalDf.groupBy("time")
      .agg(count("violationCode").alias("violations_per_date"))
      .sort(desc("violations_per_date"))
      .limit(10)
    writeIntoDFS(top10ViloationPerDate, "csv","src/main/resources/viz/topViolPerDate")(spark)
    top10ViloationPerDate.show()

    val top5ViloationPerLocation = generalDf.groupBy("location")
      .agg(count("violationCode").alias("violations_per_location"))
      .sort(desc("violations_per_location"))
      .limit(5)
     writeIntoDFS(top5ViloationPerLocation, "csv","src/main/resources/viz/topViolPerLoc")(spark)
    top5ViloationPerLocation.show()

  }
}
