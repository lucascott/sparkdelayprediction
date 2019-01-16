package sparkproject.modelling

import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tuning.CrossValidator
import sparkproject.Constants

class CVRegressionModelPipeline extends CrossValidator {
  def this(model: PipelineStage, paramGrid: Array[ParamMap], folds: Int) {
    this()
    this.setEstimator(new RegressionModelPipeline(model))
      .setEvaluator(
        new RegressionEvaluator()
          .setLabelCol(Constants.labelVariable)
          .setPredictionCol(Constants.predictionCol)
          .setMetricName(Constants.metric))
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(folds)
  }

}
