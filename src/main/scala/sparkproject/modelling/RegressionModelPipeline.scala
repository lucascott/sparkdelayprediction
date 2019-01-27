package sparkproject.modelling

import org.apache.spark.ml.feature.{StandardScaler, StringIndexer, VectorAssembler}
import org.apache.spark.ml.{PipelineStage, _}
import sparkproject.Constants

class RegressionModelPipeline extends Pipeline {


  def this(model: PipelineStage, cols: Array[String]) {
    this()
    // maps categorical attributes to int from 0 to +inf

    val indexers: Array[StringIndexer] = Constants.oneHotEncVariables.map(s =>
      new StringIndexer()
        .setInputCol(s)
        .setOutputCol(s + "Enc")
        .setHandleInvalid("keep") // alternative: "skip"
    ).toArray
    /*
    //IN CASE REMEMBER TO INCLUDE IT IN THE STAGES
    val encoder: OneHotEncoderEstimator = new OneHotEncoderEstimator()
      .setInputCols(indexers.map(x => x.getOutputCol))
      .setOutputCols(Constants.oneHotEncVariables.map(x => x + "Enc").toArray)
    */
    val assembler = new VectorAssembler()
      .setInputCols(cols)
      .setOutputCol("rawFeatures")

    val standardScaler = new StandardScaler()
      .setInputCol(assembler.getOutputCol)
      .setOutputCol("features")
      .setWithStd(true)
      .setWithMean(false)

    this.setStages(indexers ++ Array(assembler, standardScaler, model))
  }

  def this(model: PipelineStage) = this(model, Constants.featureVariables.toArray)

}
