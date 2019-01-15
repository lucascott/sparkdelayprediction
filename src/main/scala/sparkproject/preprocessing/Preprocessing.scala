package sparkproject.preprocessing

import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, SparkSession}
import sparkproject.Constants

object Preprocessing {

  def run(_ds: DataFrame, spark: SparkSession): DataFrame = {
    import spark.implicits._
    // Converts from hhmm to total minutes from midnight
    val toMin = udf(
      (hhmmString: String) => {
        val hours =
          if (hhmmString.length <= 2)
            0
          else
            hhmmString.substring(0, hhmmString.length - 2).toInt
        hours * 60 + hhmmString.takeRight(2).toInt
      })

    val ds = _ds.drop(Constants.prohibitedVariables: _*)
      .withColumn("DepMin", toMin($"DepTime"))

    // maps catergorical attributes to int from 0 to +inf
    val enc_ds: DataFrame = new EncodingPipeline(Constants.oneHotEncVariables: _*).fit(ds).transform(ds)
    enc_ds
  }

}
