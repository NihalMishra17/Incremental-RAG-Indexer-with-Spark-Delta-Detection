name := "hw2-delta-indexer"
version := "1.0"
scalaVersion := "2.12.18"

// Spark dependencies - mark as provided to exclude from JAR
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.0" ,//% "provided",
  "org.apache.spark" %% "spark-sql" % "3.5.0" ,//% "provided",
  
  // Delta Lake - MUST be included in JAR (no "provided")
  "io.delta" %% "delta-spark" % "3.2.0",
  
  // PDF processing
  "org.apache.pdfbox" % "pdfbox" % "2.0.27",
  
  // JSON - Use json4s (already in Spark, no conflicts)
  "org.json4s" %% "json4s-native" % "3.7.0-M11",
  
  // Logging
  "ch.qos.logback" % "logback-classic" % "1.2.11",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.4",
  
  // Configuration
  "com.typesafe" % "config" % "1.4.2",
  "com.github.pureconfig" %% "pureconfig" % "0.17.4",
  
  // Testing
  "org.scalatest" %% "scalatest" % "3.2.15" % Test
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

// JVM options for running locally
javaOptions ++= Seq(
  "-Xmx4G",
  "-XX:+UseG1GC"
)
