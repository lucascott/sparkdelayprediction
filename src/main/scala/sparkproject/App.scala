package sparkproject

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * @author ${user.name}
  */
object App {

  def foo(x : Array[String]) = x.foldLeft("")((a,b) => a + b)

  def main(args : Array[String]) {
    println("Arguments = " + foo(args))
    Logger.getLogger("org").setLevel(Level.WARN)

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("ML Spark Project")
      //.config("some option", "value")
      //.enableHiveSupport()
      .getOrCreate()
    val flights = spark.read.format("csv")
      .option("header", "true")
      .option("nullValue", "NA")
      .option("inferSchema", "true")
      .option("mode", "DROPMALFORMED")
      .load(args(0))
    flights.createOrReplaceTempView("flights")
    flights.printSchema()
    spark.sql("SELECT * FROM flights WHERE ArrDelay > 100").show
  }
}
