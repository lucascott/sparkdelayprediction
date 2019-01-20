package sparkproject.modelling

import org.apache.spark.ml.feature.{Normalizer, OneHotEncoderEstimator, StringIndexer, VectorAssembler}
import org.apache.spark.ml.{PipelineStage, _}
import sparkproject.Constants

class RegressionModelPipeline extends Pipeline {


  def this(model: PipelineStage, cols: Array[String]) {
    this()

    // maps catergorical attributes to int from 0 to +inf
    // TODO check if we can overwrite the vars
    val indexers: Array[StringIndexer] = Constants.oneHotEncVariables.map(s =>
      new StringIndexer()
        .setInputCol(s)
        .setOutputCol(s + "Index")
        .setHandleInvalid("keep") // alteranative: "skip"
    ).toArray

    val encoder: OneHotEncoderEstimator = new OneHotEncoderEstimator()
      .setInputCols(indexers.map(x => x.getOutputCol))
      .setOutputCols(Constants.oneHotEncVariables.map(x => x + "Enc").toArray)

    val assembler = new VectorAssembler()
      .setInputCols(cols)
      .setOutputCol("rawFeatures")

    val normalizer = new Normalizer()
      .setInputCol(assembler.getOutputCol)
      .setOutputCol("features")
      .setP(1.0)

    this.setStages(indexers ++ Array(encoder, assembler, normalizer, model))
  }

  def this(model: PipelineStage) = this(model, Constants.featureVariables.toArray) // TODO possible way, pass variables to remove

}
