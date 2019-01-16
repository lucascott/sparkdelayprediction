package sparkproject.modelling

import org.apache.spark.ml.feature.{Normalizer, VectorAssembler}
import org.apache.spark.ml.{PipelineStage, _}
import sparkproject.Constants

class RegressionModelPipeline extends Pipeline {


  def this(model: PipelineStage, cols: Array[String]) {
    this()
    val assembler = new VectorAssembler()
      .setInputCols(cols)
      .setOutputCol("rawFeatures")

    val normalizer = new Normalizer()
      .setInputCol(assembler.getOutputCol)
      .setOutputCol("features")
      .setP(1.0)

    this.setStages(Array(assembler, normalizer, model))
  }

  def this(model: PipelineStage) = this(model, Constants.featureVariables.toArray)

}
