package sparkproject

import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper {

  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Spark Flight Delay Project")
      .master("local[*]")
      .getOrCreate()

}
