package sparkproject

import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.sql.DataFrame
import sparkproject.preprocessing.Preprocessing

object EvaluateMode extends SparkSessionWrapper {
  def run(flights: DataFrame, conf: Config): Unit = {

    val test: DataFrame = Preprocessing.run(flights)

    val model = PipelineModel.read.load(conf.model)

    val models = Array(model)

    val predArr: Array[(String, DataFrame)] = models.map(x => (x.uid, x.transform(test)))

    val evaluator = new RegressionEvaluator()
      .setLabelCol(Constants.labelVariable)
      .setPredictionCol(Constants.predictionCol)
      .setMetricName(Constants.metric)

    val metricsArr: Array[(String, Double)] = predArr.map(x => (x._1, evaluator.evaluate(x._2)))
    metricsArr.foreach(x => println(s"${x._1} ${Constants.metric}: ${x._2}"))

    predArr.foreach(x => x._2.select(Constants.labelVariable, Constants.predictionCol).write
      .option("header", "true")
      .mode("overwrite")
      .csv(s"${conf.output}/${x._1}")
    )


  }

}
