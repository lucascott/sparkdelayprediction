package sparkproject

object Constants {
  val prohibitedVariables = Seq("ArrTime", "ActualElapsedTime", "AirTime", "TaxiIn", "Diverted", "CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay", "LateAircraftDelay")
  val oneHotEncVariables = Seq("UniqueCarrier", "Origin", "Dest")
}
