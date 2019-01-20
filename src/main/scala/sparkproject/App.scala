package sparkproject

import org.apache.log4j.{Level, Logger}
import scopt.OParser

/**
  * @author Luca Scotton
  * @author Alejandro Gonzales Gonzales
  */
case class Config(
                   mode: String = "",
                   input: String = "",
                   export: String = "",
                   model: String = "",
                   output: String = ""
                 )

object App extends SparkSessionWrapper {

  def main(args: Array[String]) {
    val builder = OParser.builder[Config]
    val parser = {
      import builder._
      OParser.sequence(
        programName(Constants.projectName),
        head(Constants.projectName.toLowerCase().replace(" ", ""), "1.x"),
        cmd("train")
          .action((_, c) => c.copy(mode = "train"))
          .text("Train model/models")
          .children(
            opt[String]("input")
              .abbr("i").valueName("<dataset>")
              .required()
              .action((x, c) => c.copy(input = x))
              .text("Input dataset filepath"),
            opt[String]("export")
              .abbr("e").valueName("<path>")
              .action((x, c) => c.copy(export = x))
              .text("Export model path")
          ),
        cmd("evaluate")
          .action((_, c) => c.copy(mode = "evaluate"))
          .text("Evaluate a model from disk")
          .children(
            opt[String]("model")
              .abbr("m").valueName("<model>")
              .required()
              .action((x, c) => c.copy(model = x))
              .text("model to import"),
            opt[String]("input")
              .abbr("i").valueName("<dataset>")
              .required()
              .action((x, c) => c.copy(input = x))
              .text("Input dataset path"),
            opt[String]("output")
              .abbr("o").valueName("<path>")
              .action((x, c) => c.copy(output = x))
              .text("Output dataset path")
          )
      )
    }
    val conf = OParser.parse(parser, args, Config()) match {
      case Some(config) =>
        config
      case _ =>
        sys.exit(1)
    }

    import spark.implicits._
    println(s"[INFO] Running in ${conf.mode.toUpperCase} mode")
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

    flights.printSchema

    if (conf.mode.equals("train")) {
      TrainMode.run(flights, conf)
    }
    else if (conf.mode.equals("evaluate")) {
      EvaluateMode.run(flights, conf)
    }

  }
}