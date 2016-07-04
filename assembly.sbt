import AssemblyKeys._ 

assemblySettings

jarName in assembly := "sampleKSOapp.jar"

target in assembly:= file("src/universal/sparkStreaming/sampleKSOapp")

mergeStrategy in assembly := {
  case PathList("META-INF", "jboss-beans.xml") => MergeStrategy.first
  case PathList("META-INF", "mailcap") => MergeStrategy.discard
  case PathList("META-INF", "maven", "org.slf4j", "slf4j-api", xa @ _*) => MergeStrategy.rename
  case PathList("META-INF", "ECLIPSEF.RSA") => MergeStrategy.discard
  case PathList("META-INF", "mimetypes.default") => MergeStrategy.first
  case PathList("com", "datastax", "driver", "core", "Driver.properties") => MergeStrategy.last
  case PathList("com", "esotericsoftware", "minlog", xx @ _*) => MergeStrategy.first
  case PathList("plugin.properties") => MergeStrategy.discard
  case PathList("javax", "activation", xw @ _*) => MergeStrategy.first
  case PathList("org", "apache", "hadoop", "yarn", xv @ _*) => MergeStrategy.first
  case PathList("org", "apache", "commons", xz @ _*) => MergeStrategy.first
  case PathList("org", "jboss", "netty", ya @ _*) => MergeStrategy.first
  case x => {
    val oldStrategy = (mergeStrategy in assembly).value
    oldStrategy(x)
  }
}
