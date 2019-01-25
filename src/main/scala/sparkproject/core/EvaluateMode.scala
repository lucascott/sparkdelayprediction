package sparkproject.core

import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.DataFrame
import sparkproject.evaluation.TestEvaluator
import sparkproject.preprocessing.Preprocessing
import sparkproject.{Config, Constants}

object EvaluateMode extends SparkSessionWrapper {
  def run(flights: DataFrame, conf: Config): Unit = {

    val test: DataFrame = Preprocessing.run(flights)

    val model = PipelineModel.read.load(conf.model)

    val models = Array(model)

    val predArr: Array[(String, DataFrame)] = models.map(x => (x.uid, x.transform(test)))

    TestEvaluator.evaluate(predArr)

    predArr.foreach(x => x._2.select(Constants.labelVariable, Constants.predictionCol).write
      .option("header", "true")
      .mode("overwrite")
      .csv(s"${conf.output}/${x._1}")
    )


  }

}
