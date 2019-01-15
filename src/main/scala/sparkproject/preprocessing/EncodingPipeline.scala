package sparkproject.preprocessing

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{OneHotEncoderEstimator, StringIndexer}

class EncodingPipeline extends Pipeline {
  def this(cols: String*) {
    this()
    val indexers: Seq[StringIndexer] = cols.map(s => new StringIndexer().setInputCol(s).setOutputCol(s + "Index"))
    val encoder: OneHotEncoderEstimator = new OneHotEncoderEstimator()
      .setInputCols(indexers.map(x => x.getOutputCol).toArray)
      .setOutputCols(cols.map(x => x + "Enc").toArray)
    this.setStages(indexers.toArray ++ Array(encoder))
  }
}
