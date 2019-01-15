package sparkproject

object Constants {
  val prohibitedVariables: Seq[String] = Seq("ArrTime", "ActualElapsedTime", "AirTime", "TaxiIn", "Diverted", "CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay", "LateAircraftDelay")
  val oneHotEncVariables: Seq[String] = Seq("UniqueCarrier", "Origin", "Dest")
}
