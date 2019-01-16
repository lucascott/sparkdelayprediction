package sparkproject

object Constants {
  val prohibitedVariables: Seq[String] = Seq("ArrTime", "ActualElapsedTime", "AirTime", "TaxiIn", "Diverted",
    "CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay", "LateAircraftDelay")
  val moreDroppedVariables: Seq[String] = Seq("CRSDepTime", "CRSArrTime", "FlightNum", "TailNum", "CancellationCode")
  val oneHotEncVariables: Seq[String] = Seq("UniqueCarrier", "Origin", "Dest")
  val featureVariables: Seq[String] = Seq("Year", "Month", "DayofMonth", "DayOfWeek", "DepTime", "CRSElapsedTime",
    "ArrDelay", "DepDelay", "Distance", "TaxiOut", "UniqueCarrierEnc", "OriginEnc", "DestEnc")
  val labelVariable: String = "ArrDelay"
  val predictionCol = "prediction"
  val metric: String = "rmse"
}
