name := "hw2-delta-indexer"

version := "0.1.0"

scalaVersion := "2.13.8"

// Explicit dependency management
lazy val catsVersion = "2.9.0"
lazy val circeVersion = "0.14.5"
lazy val sparkVersion = "3.3.2"
lazy val deltaVersion = "2.3.0"

libraryDependencies ++= Seq(
  // Spark
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,

  // Delta Lake
  "io.delta" %% "delta-core" % deltaVersion,

  // Cats - explicit versions to avoid conflicts
  "org.typelevel" %% "cats-core" % catsVersion,
  "org.typelevel" %% "cats-kernel" % catsVersion,

  // Circe - matching cats version
  "io.circe" %% "circe-core" % circeVersion,
  "io.circe" %% "circe-generic" % circeVersion,
  "io.circe" %% "circe-parser" % circeVersion,

  // PDF Processing
  "org.apache.pdfbox" % "pdfbox" % "2.0.31",

  // HTTP Client
  "com.softwaremill.sttp.client3" %% "core" % "3.8.15",
  "com.softwaremill.sttp.client3" %% "circe" % "3.8.15",

  // Configuration
  "com.github.pureconfig" %% "pureconfig" % "0.17.4",

  // Logging
  "ch.qos.logback" % "logback-classic" % "1.4.11",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5",

  // Lucene
  "org.apache.lucene" % "lucene-core" % "9.7.0",
  "org.apache.lucene" % "lucene-queryparser" % "9.7.0",
  "org.apache.lucene" % "lucene-analysis-common" % "9.7.0",

  // Testing
  "org.scalatest" %% "scalatest" % "3.2.17" % Test,
  "org.scalamock" %% "scalamock" % "5.2.0" % Test
)

// Dependency overrides to resolve conflicts
dependencyOverrides ++= Seq(
  "org.typelevel" %% "cats-core" % catsVersion,
  "org.typelevel" %% "cats-kernel" % catsVersion,
  "org.typelevel" %% "cats-effect" % "3.4.8"
)

// Assembly settings with proper merge strategy
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", "services", xs @ _*) => MergeStrategy.concat
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "reference.conf" => MergeStrategy.concat
  case "application.conf" => MergeStrategy.concat
  case PathList("module-info.class") => MergeStrategy.discard
  case x if x.endsWith(".proto") => MergeStrategy.first
  case x if x.contains("hadoop") => MergeStrategy.first
  case _ => MergeStrategy.first
}

// Shade cats to avoid conflicts with Spark's dependencies
assembly / assemblyShadeRules := Seq(
  ShadeRule.rename("cats.**" -> "shaded.cats.@1").inAll
)

assembly / mainClass := Some("com.cs441.hw2.SparkDeltaIndexer")
assembly / assemblyJarName := "hw2-delta-indexer.jar"

scalacOptions ++= Seq(
  "-deprecation",
  "-feature",
  "-unchecked"
)

Test / parallelExecution := false
Test / fork := true

// Add at the end of build.sbt
run / fork := true
run / connectInput := true