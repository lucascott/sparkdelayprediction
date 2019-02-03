package sparkproject.evaluation

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.sql.DataFrame
import sparkproject.Constants

object TestEvaluator {
  def evaluate(predArr: Array[(String, DataFrame)]): Unit = {
    val evaluator = new RegressionEvaluator()
      .setLabelCol(Constants.labelVariable)
      .setPredictionCol(Constants.predictionCol)
      .setMetricName(Constants.metric)

    val metricsArr: Array[(String, Double)] = predArr.map(x => (x._1, evaluator.evaluate(x._2)))
    metricsArr.foreach(x => println(s"${x._1} ${Constants.metric.toUpperCase}: ${x._2}"))
  }

  def evaluate(predDf: DataFrame): Unit = {
    this.evaluate(Array(("", predDf)))
  }
}
