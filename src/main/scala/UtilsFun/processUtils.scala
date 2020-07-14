package UtilsFun

import java.text.SimpleDateFormat

import org.apache.spark.sql.{DataFrame, SparkSession}

object processUtils {
  def readFromDFS(path:String, format:String, options:Option[Map[String,String]] = None)(implicit sparkSession: SparkSession):DataFrame = {
    options match{
      case None => sparkSession.read.format(format).option("recursiveFileLookup","true").load(path)
      case Some(opt) => sparkSession.read.format(format).options(opt).load(path)
    }
  }

  def writeIntoDFS(df:DataFrame, format:String, path:String, options:Option[Map[String,String]] = None)(implicit sparkSession: SparkSession):Unit={
    options match{
      case None => df.write.format(format).mode("append").save(path)
      case Some(opt) => df.write.format(format).mode("append").options(opt).save(path)
    }
  }
  def DateConverter(stringDate:String): Unit ={
    val format = new SimpleDateFormat("dd-MM-yyyy")
    format.parse(stringDate)

  }

}
