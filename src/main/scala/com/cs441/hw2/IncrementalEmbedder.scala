package com.cs441.hw2

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._
import java.sql.Timestamp
import java.time.Instant

class IncrementalEmbedder(spark: SparkSession, embeddingConfig: Configuration.EmbeddingConfig) extends LazyLogging {

  def generateEmbeddings(
                          chunks: Dataset[Chunk],
                          existingEmbeddings: Dataset[Embedding]
                        ): Dataset[Embedding] = {

    import spark.implicits._

    logger.info(s"Generating embeddings for ${chunks.count()} chunks")

    // Initialize client on DRIVER (not executors)
    val client = new OllamaClient(embeddingConfig.ollamaHost)

    // Get existing chunk IDs that already have embeddings
    val existingChunkIds = existingEmbeddings
      .select($"chunkId")
      .distinct()
      .as[String]
      .collect()
      .toSet

    // Remove chunks that were deleted or changed
    val chunkIds = chunks.select($"chunkId").as[String].collect().toSet
    val validExistingEmbeddings = existingEmbeddings.filter(e => chunkIds.contains(e.chunkId))

    // Count removed embeddings
    val removedCount = existingEmbeddings.count() - validExistingEmbeddings.count()
    if (removedCount > 0) {
      logger.info(s"Removed $removedCount embeddings for deleted/changed chunks")
    }

    // Filter to only new chunks
    val newChunks = chunks.filter(chunk => !existingChunkIds.contains(chunk.chunkId))

    val newChunkCount = newChunks.count()
    logger.info(s"Generating embeddings for $newChunkCount new chunks (skipping ${existingChunkIds.size} existing)")

    if (newChunkCount == 0) {
      logger.info("No new chunks to embed - returning existing embeddings")
      return validExistingEmbeddings
    }

    // ===== KEY CHANGE: Collect to driver FIRST, then process =====
    // This runs embedding generation on the DRIVER where Ollama is accessible
    val chunksToEmbed = newChunks.collect()
    logger.info(s"Collected ${chunksToEmbed.length} chunks to driver for embedding generation")

    val model = embeddingConfig.model
    val modelVersion = embeddingConfig.version

    // Generate embeddings on DRIVER (not on executors)
    val newEmbeddingsList: Seq[Embedding] = chunksToEmbed.flatMap { chunk =>
      val chunkId = chunk.chunkId
      val text = chunk.chunkText

      logger.debug(s"Generating embedding for chunk: $chunkId")

      try {
        val embedding = client.generateEmbedding(text, model)  // Pass model from config
        val embeddingId = HashUtils.generateEmbeddingId(chunkId, s"$model-$modelVersion")
        val embeddingHash = HashUtils.md5(embedding.mkString(","))

        Some(Embedding(
          embeddingId = embeddingId,
          chunkId = chunkId,
          documentId = chunk.documentId,
          embeddingVector = embedding.toList,  // List[Double]
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

    // Combine with existing valid embeddings
    validExistingEmbeddings.union(newEmbeddings)
  }
}

case class Embedding(
                      embeddingId: String,
                      chunkId: String,
                      documentId: String,
                      embeddingVector: scala.collection.immutable.Seq[Double],
                      embeddingHash: String,
                      embeddingTimestamp: Timestamp,
                      embeddingModel: String
                    )