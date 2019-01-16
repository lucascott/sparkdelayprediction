package sparkproject

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.sql.DataFrame
import sparkproject.modelling.CVRegressionModelPipeline
import sparkproject.preprocessing.Preprocessing


/**
  * @author Luca Scotton
  * @author Alejandro Gonzales Gonzales
  */
object App extends SparkSessionWrapper {

  def foo(x: Array[String]): String = x.foldLeft("")((a, b) => a + b)

  def main(args: Array[String]) {

    import spark.implicits._

    println("Arguments = " + foo(args))
    Logger.getLogger("org").setLevel(Level.WARN)

    val flights = spark.read.format("csv")
      .option("header", "true")
      .option("nullValue", "NA")
      //.option("inferSchema", "true")
      .option("mode", "DROPMALFORMED")
      .load(args(0))
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

    flights.printSchema()

    val Array(train: DataFrame, test: DataFrame) = Preprocessing.run(flights).randomSplit(Array(0.7, 0.3))

    // Linear Regression
    val lr = new LinearRegression()
      .setLabelCol(Constants.labelVariable)
      .setPredictionCol(Constants.predictionCol)
      .setMaxIter(10)

    val pgLr = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(0.3, 0.1))
      .addGrid(lr.elasticNetParam, Array(0.2, 0.5))
      .build()

    val lrModel = new CVRegressionModelPipeline(lr, pgLr, 5).fit(train)

    // Logistic Regression
    val logr = new LogisticRegression()
      .setLabelCol(Constants.labelVariable)
      .setPredictionCol(Constants.predictionCol)
      .setMaxIter(10)

    val pgLogr = new ParamGridBuilder()
      .addGrid(logr.regParam, Array(0.3, 0.1))
      .addGrid(logr.elasticNetParam, Array(0.2, 0.5))
      .build()

    val logrModel = new CVRegressionModelPipeline(lr, pgLogr, 5).fit(train)


    val models = Array(lrModel, logrModel)

    val predArr: Array[DataFrame] = models.map(x => x.transform(test))

    val evaluator = new RegressionEvaluator()
      .setLabelCol(Constants.labelVariable)
      .setPredictionCol(Constants.predictionCol)
      .setMetricName(Constants.metric)

    val metricsArr: Array[Double] = predArr.map(x => evaluator.evaluate(x))
    println(s"${Constants.metric}: " + metricsArr.mkString("\n"))


  }
}