name := "hw2-delta-indexer"

version := "0.1.0"

scalaVersion := "2.13.12"

libraryDependencies ++= Seq(
  // Spark Core
  "org.apache.spark" %% "spark-core" % "3.5.0",
  "org.apache.spark" %% "spark-sql" % "3.5.0",

  // Delta Lake (only delta-core is needed)
  "io.delta" %% "delta-core" % "2.4.0",

  // PDF Processing
  "org.apache.pdfbox" % "pdfbox" % "2.0.31",

  // HTTP Client for Ollama
  "com.softwaremill.sttp.client3" %% "core" % "3.9.5",
  "com.softwaremill.sttp.client3" %% "circe" % "3.9.5",

  // JSON Processing
  "io.circe" %% "circe-core" % "0.14.9",
  "io.circe" %% "circe-generic" % "0.14.9",
  "io.circe" %% "circe-parser" % "0.14.9",

  // Configuration
  "com.github.pureconfig" %% "pureconfig" % "0.17.6",

  // Logging
  "ch.qos.logback" % "logback-classic" % "1.5.6",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5",

  // Lucene for vector indexing
  "org.apache.lucene" % "lucene-core" % "9.10.0",
  "org.apache.lucene" % "lucene-queryparser" % "9.10.0",
  "org.apache.lucene" % "lucene-analysis-common" % "9.10.0",

  // Testing
  "org.scalatest" %% "scalatest" % "3.2.18" % Test,
  "org.scalamock" %% "scalamock" % "5.2.0" % Test
)

scalacOptions ++= Seq(
  "-deprecation",
  "-feature",
  "-unchecked"
)

Test / parallelExecution := false
Test / fork := true