package sparkproject.core

import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.regression.{GBTRegressor, GeneralizedLinearRegression, LinearRegression, RandomForestRegressor}
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
      .setMaxIter(20)

    val pgLr = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(0.3, 0.1))
      .addGrid(lr.elasticNetParam, Array(0.1, 0.5))
      .build()

    // Generalized Linear Regression
    val glr = new GeneralizedLinearRegression()
      .setLabelCol(Constants.labelVariable)
      .setPredictionCol(Constants.predictionCol)
      .setFamily("gaussian")
      .setLink("identity")
      .setMaxIter(20)

    val pgGlr = new ParamGridBuilder()
      .addGrid(glr.regParam, Array(0.3, 0.1))
      .build()

    // Random Forest Regression
    val rFor = new RandomForestRegressor()
      .setLabelCol(Constants.labelVariable)
      .setPredictionCol(Constants.predictionCol)

    val pgrFor = new ParamGridBuilder()
      .addGrid(rFor.numTrees, Array(5, 10))
      .addGrid(rFor.maxDepth, Array(5, 10))
      .build()

    // Gradient Boosting Tree Regression
    val gBoost = new GBTRegressor()
      .setLabelCol(Constants.labelVariable)
      .setPredictionCol(Constants.predictionCol)

    val pggBoost = new ParamGridBuilder()
      .addGrid(gBoost.maxIter, Array(1, 2))
      .addGrid(gBoost.maxDepth, Array(5, 10))
      .build()

    val models: Array[(String, CrossValidatorModel)] = RegressionTrainFactory.setTrainDataset(train).train(Array(
      //("LM", lr, pgLr),
      ("GLM", glr, pgGlr)
      //("RF", rFor, pgrFor)
      //("GB", gBoost, pggBoost)
    ))

    val predArr: Array[(String, DataFrame)] = models.map(x => (x._1, x._2.transform(test)))

    TestEvaluator.evaluate(predArr)

    // Now we can optionally save the fitted pipeline to disk
    if (!conf.export.isEmpty) {
      val t: Long = System.currentTimeMillis / 1000
      models.foreach(x => x._2.bestModel.asInstanceOf[PipelineModel].write.overwrite().save(s"${conf.export}/${x._1}_$t"))
    }
  }

}
