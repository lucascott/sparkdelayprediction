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


    if (conf.predict) println("[INFO] Predict only.") else TestEvaluator.evaluate(predArr)

    val outVars: Seq[String] = if (conf.predict) Seq(Constants.flightId, Constants.predictionCol) else Seq(Constants.flightId, Constants.labelVariable, Constants.predictionCol)
    predArr.foreach(x => x._2
      .select(outVars.head, outVars.tail: _*)
      .write
      .option("header", "true")
      .mode("overwrite")
      .csv(s"${conf.output}/${x._1}_pred")
    )


  }

}
