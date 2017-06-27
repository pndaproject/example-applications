name := "PNDA KSO sample app"

version := "1.0.2"

scalaVersion := "2.10.5"

enablePlugins(UniversalPlugin)

packageZipTarball in Universal := {
  val originalFileName = (packageZipTarball in Universal).value
  val (base, ext) = originalFileName.baseAndExt
  val newFileName = file(originalFileName.getParent) / (base + ".tar.gz")
  IO.move(originalFileName, newFileName)
  newFileName
}

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "1.6.0" % "provided",
    "org.apache.spark" %% "spark-streaming" % "1.6.0" % "provided",
    "org.apache.spark" %% "spark-streaming-kafka" % "1.6.0" % "provided",
    "org.apache.kafka" %% "kafka" % "0.10.0.1",
    "org.apache.avro" % "avro" % "1.7.7",
    "org.codehaus.jackson" % "jackson-mapper-asl" % "1.9.13",
    "joda-time" % "joda-time" % "2.7",
    "log4j" % "log4j" % "1.2.14",
    "org.apache.httpcomponents" % "httpcore" % "4.2.5" % "provided",
    "org.apache.httpcomponents" % "httpclient" % "4.2.5" % "provided"
)
