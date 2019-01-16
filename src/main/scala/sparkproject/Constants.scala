package sparkproject

object Constants {
  val prohibitedVariables: Seq[String] = Seq("ArrTime", "ActualElapsedTime", "AirTime", "TaxiIn", "Diverted",
    "CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay", "LateAircraftDelay")
  val moreDroppedVariables: Seq[String] = Seq("Year", "CRSDepTime", "CRSArrTime", "FlightNum", "TailNum", "CancellationCode")
  val oneHotEncVariables: Seq[String] = Seq("UniqueCarrier", "Origin", "Dest")
  val labelVariable: String = "ArrDelay"
  val metric: String = "rmse"
}
