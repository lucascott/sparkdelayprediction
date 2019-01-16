package sparkproject.preprocessing

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf
import sparkproject.{Constants, SparkSessionWrapper}

object Preprocessing extends SparkSessionWrapper {

  def run(_ds: DataFrame): DataFrame = {
    import spark.implicits._

    var ds = _ds.drop(Constants.prohibitedVariables: _*).drop(Constants.moreDroppedVariables: _*)

    ds = ds.filter('Diverted === 0)
    ds = ds.filter('Cancelled === 0).drop("Cancelled")

    val nullValuesDf = ds.filter('ArrDelay.isNull)
    if (nullValuesDf.count() > 0) {
      println("We still have null values! Please check why! We already have removed the expected source of nulls.")
      nullValuesDf.show()
      println("Removing remaining null values")
      ds = ds.filter('ArrDelay.isNotNull)
    }
    else {
      println("No more null values in target column")
    }
    // Converts from hhmm to total minutes from midnight
    val toMin = udf((hhmmString: String) => {
      val hours =
        if (hhmmString.length <= 2)
          0
        else
          hhmmString.substring(0, hhmmString.length - 2).toInt
      hours * 60 + hhmmString.takeRight(2).toInt
    })

    ds = ds.withColumn("DepTime", toMin('DepTime))
      .withColumn("DepTime", 'DepTime.cast("int"))

    // maps catergorical attributes to int from 0 to +inf
    val enc_ds: DataFrame = new EncodingPipeline(Constants.oneHotEncVariables: _*).fit(ds).transform(ds)
      .drop(Constants.oneHotEncVariables: _*).drop(Constants.oneHotEncVariables.map(x => x + "Index"): _*)
    enc_ds
  }

}
