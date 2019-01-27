package sparkproject.core

import org.apache.spark.sql.SparkSession
import sparkproject.Constants

trait SparkSessionWrapper {

  val spark: SparkSession =
    SparkSession
      .builder()
      .appName(Constants.projectName)
      //s.master("local[*]")
      .getOrCreate()
}
