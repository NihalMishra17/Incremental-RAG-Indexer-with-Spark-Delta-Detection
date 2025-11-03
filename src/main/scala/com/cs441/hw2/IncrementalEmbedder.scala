package com.cs441.hw2

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._
import java.sql.Timestamp
import java.time.Instant

/**
 * Generates embeddings incrementally for new/changed chunks only.
 *
 * Design rationale:
 * - Runs on driver node to access localhost Ollama service
 * - Filters out chunks that already have embeddings (idempotent)
 * - Removes stale embeddings for deleted/changed chunks
 * - Collects chunks to driver before embedding to avoid executor network issues
 */
class IncrementalEmbedder(spark: SparkSession, embeddingConfig: Configuration.EmbeddingConfig) extends LazyLogging {

  /**
   * Generates embeddings for chunks that don't already have them.
   * Returns union of existing valid embeddings + newly generated embeddings.
   */
  def generateEmbeddings(
                          chunks: Dataset[Chunk],
                          existingEmbeddings: Dataset[Embedding]
                        ): Dataset[Embedding] = {

    import spark.implicits._

    logger.info(s"Generating embeddings for ${chunks.count()} chunks")

    // Initialize Ollama client on driver node
    val client = new OllamaClient(embeddingConfig.ollamaHost)

    // Get set of chunk IDs that already have embeddings
    val existingChunkIds = existingEmbeddings
      .select($"chunkId")
      .distinct()
      .as[String]
      .collect()
      .toSet

    // Remove embeddings for chunks that no longer exist (deleted or changed documents)
    val chunkIds = chunks.select($"chunkId").as[String].collect().toSet
    val validExistingEmbeddings = existingEmbeddings.filter(e => chunkIds.contains(e.chunkId))

    val removedCount = existingEmbeddings.count() - validExistingEmbeddings.count()
    if (removedCount > 0) {
      logger.info(s"Removed $removedCount embeddings for deleted/changed chunks")
    }

    // Filter to only chunks without embeddings
    val newChunks = chunks.filter(chunk => !existingChunkIds.contains(chunk.chunkId))

    val newChunkCount = newChunks.count()
    logger.info(s"Generating embeddings for $newChunkCount new chunks (skipping ${existingChunkIds.size} existing)")

    if (newChunkCount == 0) {
      logger.info("No new chunks to embed - returning existing embeddings")
      return validExistingEmbeddings
    }

    // Collect chunks to driver for serial processing
    // This avoids network/authentication issues with executors accessing Ollama
    val chunksToEmbed = newChunks.collect()
    logger.info(s"Collected ${chunksToEmbed.length} chunks to driver for embedding generation")

    val model = embeddingConfig.model
    val modelVersion = embeddingConfig.version

    // Generate embeddings on driver using Ollama API
    val newEmbeddingsList: Seq[Embedding] = chunksToEmbed.flatMap { chunk =>
      val chunkId = chunk.chunkId
      val text = chunk.chunkText

      logger.debug(s"Generating embedding for chunk: $chunkId")

      try {
        // Call Ollama API to generate embedding vector
        val embedding = client.generateEmbedding(text, model)
        val embeddingId = HashUtils.generateEmbeddingId(chunkId, s"$model-$modelVersion")
        val embeddingHash = HashUtils.md5(embedding.mkString(","))

        Some(Embedding(
          embeddingId = embeddingId,
          chunkId = chunkId,
          documentId = chunk.documentId,
          embeddingVector = embedding.toList,
          embeddingHash = embeddingHash,
          embeddingTimestamp = Timestamp.from(Instant.now()),
          embeddingModel = s"$model-$modelVersion"
        ))
      } catch {
        case e: Exception =>
          logger.error(s"Exception generating embedding for chunk $chunkId: ${e.getMessage}")
          None
      }
    }

    val newEmbeddings = spark.createDataset(newEmbeddingsList)
    logger.info(s"Generated ${newEmbeddings.count()} new embeddings")

    // Merge new embeddings with existing valid ones
    validExistingEmbeddings.union(newEmbeddings)
  }
}

/**
 * Represents a chunk embedding with vector, metadata, and provenance.
 */
case class Embedding(
                      embeddingId: String,
                      chunkId: String,
                      documentId: String,
                      embeddingVector: scala.collection.immutable.Seq[Double],
                      embeddingHash: String,
                      embeddingTimestamp: Timestamp,
                      embeddingModel: String
                    )