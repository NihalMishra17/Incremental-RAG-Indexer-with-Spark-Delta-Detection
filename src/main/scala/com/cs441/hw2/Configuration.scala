package com.cs441.hw2

import pureconfig._
import pureconfig.generic.auto._

/**
 * Configuration for the delta indexer application.
 */
object Configuration {

  case class ChunkingConfig(
                             chunkSize: Int,
                             overlap: Int,
                             version: String
                           )

  case class EmbeddingConfig(
                              model: String,
                              version: String,
                              batchSize: Int,
                              dimension: Int,
                              ollamaHost: String
                            )

  case class IndexConfig(
                          numShards: Int,
                          publishStrategy: String
                        )

  case class SparkConfig(
                          appName: String,
                          master: String,
                          checkpointDir: String
                        )

  case class DeltaIndexerConfig(
                                 inputDir: String,
                                 outputDir: String,
                                 documentsTable: String,
                                 chunksTable: String,
                                 embeddingsTable: String,
                                 indexTable: String,
                                 chunking: ChunkingConfig,
                                 embedding: EmbeddingConfig,
                                 index: IndexConfig,
                                 spark: SparkConfig
                               )

  def load(): Either[Throwable, DeltaIndexerConfig] = {
    ConfigSource.default.at("delta-indexer").load[DeltaIndexerConfig].left.map { failures =>
      new RuntimeException(s"Failed to load configuration: ${failures.prettyPrint()}")
    }
  }

  def loadOrThrow(): DeltaIndexerConfig = {
    load() match {
      case Right(config) => config
      case Left(error) => throw error
    }
  }
}