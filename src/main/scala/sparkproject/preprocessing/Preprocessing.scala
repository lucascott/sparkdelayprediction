package sparkproject.preprocessing

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf
import sparkproject.Constants
import sparkproject.core.SparkSessionWrapper

object Preprocessing extends SparkSessionWrapper {

  def run(_ds: DataFrame): DataFrame = {

    import spark.implicits._

    var ds = _ds.drop(Constants.prohibitedVariables: _*).drop(Constants.moreDroppedVariables: _*)

    ds = ds.filter('Diverted === 0).drop("Diverted")
    ds = ds.filter('Cancelled === 0).drop("Cancelled")

    val nullValuesDf = ds.filter('ArrDelay.isNull)
    if (nullValuesDf.count() > 0) {
      println("[INFO] There are still have null values.")
      nullValuesDf.show()
      println(s"[INFO] Removing rows with remaining null values on")
      ds = ds.filter('ArrDelay.isNotNull)
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

    ds
  }

}
