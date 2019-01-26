package sparkproject

object Constants {
  val projectName = "Spark Flight Delay Project"
  val flightId = "FlightNum"
  val prohibitedVariables: Seq[String] = Seq("ArrTime", "ActualElapsedTime", "AirTime", "TaxiIn", "Diverted",
    "CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay", "LateAircraftDelay")
  val moreDroppedVariables: Seq[String] = Seq("CRSDepTime", "CRSArrTime", "TailNum", "CancellationCode")
  val oneHotEncVariables: Seq[String] = Seq("UniqueCarrier", "Origin", "Dest")
  val featureVariables: Seq[String] = Seq("Year", "Month", "DayofMonth", "DayOfWeek", "DepTime",
    "DepDelay", "TaxiOut", "UniqueCarrierEnc") //"CRSElapsedTime", "OriginEnc", "DestEnc", "Distance")
  val labelVariable: String = "ArrDelay"
  val predictionCol = "prediction"
  val metric: String = "rmse"
  val trainTestSplit = Array(0.7, 0.3)
  val cvFolds = 5
}
