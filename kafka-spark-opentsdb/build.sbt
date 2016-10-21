name := "PNDA KSO sample app"

version := "0.0.4"

scalaVersion := "2.10.4"

enablePlugins(UniversalPlugin)

packageZipTarball in Universal := {
  val originalFileName = (packageZipTarball in Universal).value
  val (base, ext) = originalFileName.baseAndExt
  val newFileName = file(originalFileName.getParent) / (base + ".tar.gz")
  IO.move(originalFileName, newFileName)
  newFileName
}

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "1.3.0" % "provided",
    "org.apache.spark" %% "spark-streaming" % "1.3.0" % "provided",
    "org.apache.spark" %% "spark-streaming-kafka" % "1.3.0" % "provided",
    "org.apache.kafka" %% "kafka" % "0.8.2.2",
    "org.apache.avro" % "avro" % "1.7.7",
    "org.codehaus.jackson" % "jackson-mapper-asl" % "1.9.13",
    "joda-time" % "joda-time" % "2.7",
    "log4j" % "log4j" % "1.2.14",
    "org.scalaj" %% "scalaj-http" % "1.1.5",
    "org.apache.httpcomponents" % "httpcore" % "4.2.5",
    "org.apache.httpcomponents" % "httpclient" % "4.2.5"
)

