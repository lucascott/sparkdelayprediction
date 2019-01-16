package sparkproject.modelling

import org.apache.spark.ml.feature.{Normalizer, VectorAssembler}
import org.apache.spark.ml.{PipelineStage, _}
import org.apache.spark.sql.Dataset
import sparkproject.Constants

class RegressionModelPipeline extends Pipeline {
  var model: PipelineStage = _

  def this(model: PipelineStage) {
    this()
    this.model = model
  }

  override def fit(dataset: Dataset[_]): PipelineModel = {

    val targetIndex = dataset.columns.indexOf(Constants.labelVariable)

    val assembler = new VectorAssembler()
      .setInputCols(dataset.columns.drop(targetIndex))
      .setOutputCol("rawFeatures")

    val label = new VectorAssembler()
      .setInputCols(Array(Constants.labelVariable))
      .setOutputCol("label")

    val normalizer = new Normalizer()
      .setInputCol("rawFeatures")
      .setOutputCol("features")
      .setP(1.0)

    this.setStages(Array(assembler, label, normalizer, model))

    super.fit(dataset)
  }

}
