package sparkproject

import org.apache.log4j.{Level, Logger}
import sparkproject.preprocessing.Preprocessing


/**
  * @author Luca Scotton
  * @author Alejandro Gonzales Gonzales
  */
object App extends SparkSessionWrapper {

  def foo(x: Array[String]): String = x.foldLeft("")((a, b) => a + b)

  def main(args: Array[String]) {

    //import spark.implicits._

    println("Arguments = " + foo(args))
    Logger.getLogger("org").setLevel(Level.WARN)

    val flights = spark.read.format("csv")
      .option("header", "true")
      .option("nullValue", "ja")
      //.option("inferSchema", "true")
      .option("mode", "DROPMALFORMED")
      .load(args(0))

    flights.printSchema()

    val cleaned_flights = Preprocessing.run(flights, spark) //.randomSplit(Array(0.7, 0.3))
    cleaned_flights.show
  }
}