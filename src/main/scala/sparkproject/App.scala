package sparkproject

import org.apache.log4j.{Level, Logger}
import sparkproject.core.{PredictMode, SparkSessionWrapper, TrainMode}
import sparkproject.utils.ArgsParser

/**
  * @author Luca Scotton
  * @author Alejandro Gonzales Gonzales
  */
case class Config(
                   mode: String = "",
                   input: String = "",
                   export: String = "",
                   eval: Boolean = false,
                   model: String = "",
                   output: String = ""
                 )

object App extends SparkSessionWrapper {

  def main(args: Array[String]) {

    val conf = ArgsParser.parse(args)

    import spark.implicits._

    println(s"[INFO] Running in ${conf.mode.toUpperCase} mode")
    if (conf.mode == "predict") println(s"[INFO] Evaluation is set to ${if (conf.eval) "ON" else "OFF"}")
    if (conf.mode == "train") println(s"[INFO] Models' exportation is set to ${if (!conf.export.isEmpty) "ON" else "OFF"}")
    println("[INFO] Reading file: " + conf.input)
    Logger.getLogger("org").setLevel(Level.WARN)

    val flights = spark.read.format("csv")
      .option("header", "true")
      .option("nullValue", "NA")
      //.option("inferSchema", "true")
      .option("mode", "DROPMALFORMED")
      .load(conf.input)
      .withColumn("Year", $"Year".cast("int"))
      .withColumn("Month", $"Month".cast("int"))
      .withColumn("DayofMonth", $"DayofMonth".cast("int"))
      .withColumn("DayOfWeek", $"DayOfWeek".cast("int"))
      // DepTime sting convertedToMin (real dep time)
      // CRSDepTime string (scheduled dep time) redundant with DepDelay
      // ArrTime prohib
      // CRSArrTime string (scheduled arr time) redundant with CRSElapsedTime
      // UniqueCarrier string oneHot
      // FlightNum string removed but possible correlations
      // TailNum string removed
      // ActualElapsedTime prohib
      .withColumn("CRSElapsedTime", $"CRSElapsedTime".cast("int"))
      // AirTime prohib
      .withColumn("DepDelay", $"DepDelay".cast("int"))
      // Origin string oneHot
      // Dest string oneHot
      .withColumn("Distance", $"Distance".cast("int"))
      // TaxiIn prohib
      .withColumn("TaxiOut", $"TaxiOut".cast("int"))
      .withColumn("Cancelled", $"Cancelled".cast("int"))
      // CancellationCode string removed
      .withColumn("Diverted", $"Diverted".cast("int"))
      // other prohib cols
      // LABEL
      .withColumn("ArrDelay", $"ArrDelay".cast("int"))

    if (conf.mode.equals("train")) {
      TrainMode.run(flights, conf)
    }
    else if (conf.mode.equals("predict")) {
      PredictMode.run(flights, conf)
    }

  }
}