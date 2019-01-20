package sparkproject

import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper {

  val spark: SparkSession =
    SparkSession
      .builder()
      .appName(Constants.projectName)
      .master("local[*]")
      .getOrCreate()
}
