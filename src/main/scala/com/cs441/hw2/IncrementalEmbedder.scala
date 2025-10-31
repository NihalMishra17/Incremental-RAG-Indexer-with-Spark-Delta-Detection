package com.cs441.hw2

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{Dataset, SparkSession}

import java.sql.Timestamp
import java.time.Instant

case class Embedding(
                      embeddingId: String,
                      chunkId: String,
                      documentId: String,
                      embeddingVector: Seq[Double],
                      embeddingHash: String,
                      embeddingTimestamp: Timestamp,
                      embeddingModel: String
                    )

class IncrementalEmbedder(spark: SparkSession, config: Configuration.EmbeddingConfig) extends LazyLogging {
  import spark.implicits._

  def generateEmbeddings(
                          chunks: Dataset[Chunk],
                          existingEmbeddings: Dataset[Embedding]
                        ): Dataset[Embedding] = {
    logger.info(s"Generating embeddings for ${chunks.count()} chunks")

    // Filter existing embeddings to only keep those whose chunks still exist
    val validChunkIds = chunks.select("chunkId").as[String].collect().toSet
    val filteredExistingEmbeddings = existingEmbeddings.filter(emb => validChunkIds.contains(emb.chunkId))

    val filteredCount = filteredExistingEmbeddings.count()
    val removedCount = existingEmbeddings.count() - filteredCount
    if (removedCount > 0) {
      logger.info(s"Removed $removedCount embeddings for deleted/changed chunks")
    }

    val existingChunkIds = filteredExistingEmbeddings.select("chunkId").as[String].collect().toSet
    val newChunks = chunks.filter(chunk => !existingChunkIds.contains(chunk.chunkId))

    val newChunksCount = newChunks.count()
    val skippedCount = existingChunkIds.size

    logger.info(s"Generating embeddings for $newChunksCount new chunks (skipping $skippedCount existing)")

    if (newChunksCount == 0) {
      logger.info("No new chunks to embed - returning existing embeddings")
      return filteredExistingEmbeddings
    }

    // Capture only serializable values (not the whole config object)
    val ollamaHost = sys.env.getOrElse("OLLAMA_HOST", config.ollamaHost)
    val model = config.model

    // Broadcast values for use in executors
    val broadcastHost = spark.sparkContext.broadcast(ollamaHost)
    val broadcastModel = spark.sparkContext.broadcast(model)

    // Generate embeddings in parallel using mapPartitions
    val embeddingsRDD = newChunks.rdd.mapPartitions { partition =>
      // Get broadcasted values in executor
      val host = broadcastHost.value
      val modelName = broadcastModel.value

      // Create one OllamaClient per partition (not per chunk)
      val client = new OllamaClient(host)

      partition.map { chunk =>
        try {
          // Generate embedding - returns Array[Double]
          val embedding: Array[Double] = client.generateEmbedding(chunk.chunkText, modelName)

          // Create hash and embedding object
          val embeddingHash = HashUtils.md5(embedding.mkString(","))
          val embeddingId = s"${chunk.chunkId}_${embeddingHash.take(8)}"

          Embedding(
            embeddingId = embeddingId,
            chunkId = chunk.chunkId,
            documentId = chunk.documentId,
            embeddingVector = embedding.toSeq,
            embeddingHash = embeddingHash,
            embeddingTimestamp = Timestamp.from(Instant.now()),
            embeddingModel = modelName
          )
        } catch {
          case e: Exception =>
            // Can't use logger in executor - print to stderr
            System.err.println(s"Failed to generate embedding for chunk ${chunk.chunkId}: ${e.getMessage}")
            throw e
        }
      }
    }

    val newEmbeddings = spark.createDataset(embeddingsRDD)
    logger.info(s"Generated ${newEmbeddings.count()} new embeddings")

    filteredExistingEmbeddings.union(newEmbeddings)
  }
}