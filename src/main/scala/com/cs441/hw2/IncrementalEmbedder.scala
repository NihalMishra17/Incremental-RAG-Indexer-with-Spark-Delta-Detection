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

    // Get existing chunk IDs that already have embeddings
    val existingChunkIds = existingEmbeddings
      .select($"chunkId")
      .distinct()
      .as[String]
      .collect()
      .toSet

    // Filter to only new chunks
    val newChunks = chunks.filter(chunk => !existingChunkIds.contains(chunk.chunkId))

    val newChunkCount = newChunks.count()
    logger.info(s"Generating embeddings for $newChunkCount new chunks (skipping ${existingChunkIds.size} existing)")

    if (newChunkCount == 0) {
      logger.info("No new chunks to embed")
      return existingEmbeddings
    }

    // Capture only primitives to avoid serialization issues
    val model = embeddingConfig.model
    val modelVersion = embeddingConfig.version
    val ollamaHost = embeddingConfig.ollamaHost

    val embeddingsRDD = newChunks.rdd.mapPartitions { partition =>
      // Create client inside mapPartitions to avoid serialization
      val client = new OllamaClient(ollamaHost, model)

      partition.flatMap { chunk =>
        val chunkId = chunk.chunkId
        val text = chunk.chunkText

        client.generateEmbedding(text) match {
          case Right(embedding) =>
            val embeddingId = HashUtils.generateEmbeddingId(chunkId, s"$model-$modelVersion")
            val embeddingHash = HashUtils.md5(embedding.mkString(","))

            Some(Embedding(
              embeddingId = embeddingId,
              chunkId = chunkId,
              embeddingVector = embedding.toList,
              modelName = model,
              modelVersion = modelVersion,
              embeddingTimestamp = Timestamp.from(Instant.now()),
              embeddingHash = embeddingHash
            ))

          case Left(error) =>
            // Can't use logger here as it's not serializable
            println(s"ERROR: Failed to generate embedding for chunk $chunkId: $error")
            None
        }
      }
    }

    val newEmbeddings = spark.createDataset(embeddingsRDD.collect().toSeq)

    logger.info(s"Generated ${newEmbeddings.count()} new embeddings")

    // Combine with existing
    existingEmbeddings.union(newEmbeddings)
  }
}

case class Embedding(
                      embeddingId: String,
                      chunkId: String,
                      embeddingVector: scala.collection.immutable.Seq[Float],
                      modelName: String,
                      modelVersion: String,
                      embeddingTimestamp: Timestamp,
                      embeddingHash: String
                    )