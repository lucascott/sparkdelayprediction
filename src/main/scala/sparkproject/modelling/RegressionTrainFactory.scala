package sparkproject.modelling

import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tuning.CrossValidatorModel
import org.apache.spark.ml.{PipelineModel, PipelineStage}
import org.apache.spark.sql.DataFrame
import sparkproject.Constants

object RegressionTrainFactory {

  private var trainDf: DataFrame = _

  def setTrainDataset(ds: DataFrame): RegressionTrainFactory.type = {
    trainDf = ds
    this
  }

  def train(models: Array[(String, PipelineStage, Array[ParamMap])]): Array[(String,CrossValidatorModel)] = {
    if (this.trainDf == null) {
      sys.error("[ERROR] It is necessary to set the dataset first. Use setTrainDataset() function.")
    }
    models.map(x => (x._1, new CVRegressionModelPipeline(x._2, x._3, Constants.cvFolds).fit(trainDf)))
  }

  def train(models: Array[(PipelineStage, Array[ParamMap])]): Array[CrossValidatorModel] = {
    if (this.trainDf == null) {
      sys.error("[ERROR] It is necessary to set the dataset first. Use setTrainDataset() function.")
    }
    models.map(x => new CVRegressionModelPipeline(x._1, x._2, Constants.cvFolds).fit(trainDf))
  }

  def train(models: Array[PipelineStage]): Array[PipelineModel] = {
    if (this.trainDf == null) {
      sys.error("[ERROR] It is necessary to set the dataset first. Use setTrainDataset() function.")
    }
    models.map(x => new RegressionModelPipeline(x).fit(trainDf))
  }
}
