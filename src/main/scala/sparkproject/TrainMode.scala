package sparkproject

import java.time.Instant

import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.sql.DataFrame
import sparkproject.modelling.CVRegressionModelPipeline
import sparkproject.preprocessing.Preprocessing

object TrainMode extends SparkSessionWrapper {
  def run(flights: DataFrame, conf: Config): Unit = {
    val Array(train: DataFrame, test: DataFrame) = Preprocessing.run(flights).randomSplit(Array(0.7, 0.3))

    // Linear Regression
    val lr = new LinearRegression()
      .setLabelCol(Constants.labelVariable)
      .setPredictionCol(Constants.predictionCol)
      .setMaxIter(10)

    val pgLr = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(0.1))
      .addGrid(lr.elasticNetParam, Array(0.5))
      .build()

    val lrModel = new CVRegressionModelPipeline(lr, pgLr, 5).fit(train).bestModel.asInstanceOf[PipelineModel]

    // Logistic Regression
    val logr = new LogisticRegression()
      .setLabelCol(Constants.labelVariable)
      .setPredictionCol(Constants.predictionCol)
      .setMaxIter(10)

    val pgLogr = new ParamGridBuilder()
      .addGrid(logr.regParam, Array(0.3))
      .addGrid(logr.elasticNetParam, Array(0.2))
      .build()

    val logrModel = new CVRegressionModelPipeline(lr, pgLogr, 5).fit(train).bestModel.asInstanceOf[PipelineModel]


    val models = Array(lrModel, logrModel)

    val predArr: Array[(String, DataFrame)] = models.map(x => (x.uid, x.transform(test)))

    val evaluator = new RegressionEvaluator()
      .setLabelCol(Constants.labelVariable)
      .setPredictionCol(Constants.predictionCol)
      .setMetricName(Constants.metric)

    val metricsArr: Array[(String, Double)] = predArr.map(x => (x._1, evaluator.evaluate(x._2)))
    metricsArr.foreach(x => println(s"${x._1} ${Constants.metric}: ${x._2}"))

    if (!conf.export.isEmpty) {
      // Now we can optionally save the fitted pipeline to disk
      val now: Long = Instant.now.getEpochSecond
      models.foreach(x => x.write.overwrite().save(s"${conf.export}/${x.uid}_$now"))
    }
  }

}
