package sparkproject.core

import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.regression.{GBTRegressor, LinearRegression, RandomForestRegressor}
import org.apache.spark.ml.tuning.{CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.sql.DataFrame
import sparkproject.evaluation.TestEvaluator
import sparkproject.modelling.RegressionTrainFactory
import sparkproject.preprocessing.Preprocessing
import sparkproject.{Config, Constants}

object TrainMode extends SparkSessionWrapper {
  def run(flights: DataFrame, conf: Config): Unit = {

    val Array(train: DataFrame, test: DataFrame) = Preprocessing.run(flights).randomSplit(Constants.trainTestSplit)

    // Linear Regression
    val lr = new LinearRegression()
      .setLabelCol(Constants.labelVariable)
      .setPredictionCol(Constants.predictionCol)
      .setMaxIter(10)

    val pgLr = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(0.1, 0.01))
      .addGrid(lr.elasticNetParam, Array(0.5))
      .build()

    // Random Forest Regression
    val rFor = new RandomForestRegressor()
      .setLabelCol(Constants.labelVariable)
      .setPredictionCol(Constants.predictionCol)

    val pgrFor = new ParamGridBuilder()
      .addGrid(rFor.numTrees, Array(2))
      .addGrid(rFor.maxDepth, Array(20))
      .build()

    // Gradient Boosting Tree Regression
    val gBoost = new GBTRegressor()
      .setLabelCol(Constants.labelVariable)
      .setPredictionCol(Constants.predictionCol)

    val pggBoost = new ParamGridBuilder()
      .addGrid(gBoost.maxIter, Array(10))
      .addGrid(gBoost.maxDepth, Array(5, 10, 20, 30))
      .build()

    val models: Array[CrossValidatorModel] = RegressionTrainFactory.setTrainDataset(train).train(Array(
      (lr, pgLr),
      (rFor, pgrFor)
        (gBoost, pggBoost)
    ))

    val predArr: Array[(String, DataFrame)] = models.map(x => (x.uid, x.transform(test)))

    TestEvaluator.evaluate(predArr)

    // Now we can optionally save the fitted pipeline to disk
    if (!conf.export.isEmpty) {
      models.foreach(x => x.bestModel.asInstanceOf[PipelineModel].write.overwrite().save(s"${conf.export}/${x.uid}"))
    }
  }

}
