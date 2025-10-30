name := "hw2-delta-indexer"
version := "1.0"
scalaVersion := "2.12.18"

libraryDependencies ++= Seq(
  // Spark dependencies - mark as provided to exclude from JAR
  "org.apache.spark" %% "spark-core" % "3.5.0",// % "provided",
  "org.apache.spark" %% "spark-sql" % "3.5.0",// % "provided",
  
  // Delta Lake - MUST be included in JAR (no "provided")
  "io.delta" %% "delta-spark" % "3.2.0",
  
  // PDF processing
  "org.apache.pdfbox" % "pdfbox" % "2.0.27",
  
  // Logging
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5",
  "ch.qos.logback" % "logback-classic" % "1.4.11",
  
  // Config
  "com.typesafe" % "config" % "1.4.2",
  "com.github.pureconfig" %% "pureconfig" % "0.17.4",
  
  // HTTP client for Ollama
  "com.softwaremill.sttp.client3" %% "core" % "3.8.15",
  "com.softwaremill.sttp.client3" %% "circe" % "3.8.15",
  
  // JSON
  "io.circe" %% "circe-core" % "0.14.5",
  "io.circe" %% "circe-generic" % "0.14.5",
  "io.circe" %% "circe-parser" % "0.14.5"
)

// Assembly settings
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => xs match {
    case "MANIFEST.MF" :: Nil => MergeStrategy.discard
    case "services" :: _ => MergeStrategy.concat
    case _ => MergeStrategy.discard
  }
  case "reference.conf" => MergeStrategy.concat
  case "application.conf" => MergeStrategy.concat
  case _ => MergeStrategy.first
}

assembly / assemblyJarName := "hw2-delta-indexer.jar"

fork := true
