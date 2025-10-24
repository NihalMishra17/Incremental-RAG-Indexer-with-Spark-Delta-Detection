package com.cs441.hw2

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.sql.Timestamp
import java.time.Instant

// Define Embedding at package level
case class Embedding(
                      embeddingId: String,
                      chunkId: String,
                      embeddingVector: Seq[Float],
                      modelName: String,
                      modelVersion: String,
                      embeddingTimestamp: Timestamp,
                      embeddingHash: String
                    )

/**
 * Handles incremental embedding generation.
 */
class IncrementalEmbedder(
                           spark: SparkSession,
                           config: Configuration.EmbeddingConfig
                         ) extends LazyLogging {
  import spark.implicits._

  def generateEmbeddings(chunks: DataFrame, existingEmbeddings: DataFrame): DataFrame = {
    logger.info("Starting incremental embedding generation")

    val newChunks = if (existingEmbeddings.isEmpty) {
      logger.info("No existing embeddings - generating for all chunks")
      chunks
    } else {
      logger.info("Finding chunks that need embeddings")
      chunks
        .join(existingEmbeddings, chunks("chunkId") === existingEmbeddings("chunkId"), "left_anti")
    }

    val newChunksCount = newChunks.count()
    logger.info(s"Generating embeddings for $newChunksCount new chunks")

    if (newChunksCount == 0) {
      logger.info("No new chunks to embed")
      return spark.emptyDataFrame
    }

    // Extract config values to primitives to avoid serializing the entire object
    val ollamaHost = config.ollamaHost
    val model = config.model
    val modelVersion = config.version
    val batchSize = config.batchSize

    // Generate embeddings in parallel using mapPartitions
    val embeddingsRDD = newChunks.rdd.mapPartitions { partition =>
      // Create client inside the partition (on executor)
      val client = new OllamaClient(ollamaHost, model)

      val results = partition.grouped(batchSize).flatMap { batch =>
        val texts = batch.map(row => (
          row.getAs[String]("chunkId"),
          row.getAs[String]("chunkText")
        )).toList

        // Use object method for logging to avoid serialization
        IncrementalEmbedder.logBatch(texts.size)

        texts.flatMap { case (chunkId, text) =>
          client.generateEmbedding(text) match {
            case Right(embedding) =>
              val embeddingId = HashUtils.generateEmbeddingId(chunkId, s"$model-$modelVersion")
              val embeddingHash = HashUtils.md5(embedding.mkString(","))

              Some(Embedding(
                embeddingId = embeddingId,
                chunkId = chunkId,
                embeddingVector = embedding.toSeq,
                modelName = model,
                modelVersion = modelVersion,
                embeddingTimestamp = Timestamp.from(Instant.now()),
                embeddingHash = embeddingHash
              ))
            case Left(error) =>
              IncrementalEmbedder.logFailure(chunkId, error)
              None
          }
        }
      }

      client.close()
      results
    }

    val embeddingsDF = embeddingsRDD.toDF()
    val generatedCount = embeddingsDF.count()
    logger.info(s"Successfully generated $generatedCount embeddings out of $newChunksCount chunks")

    embeddingsDF
  }

  def mergeEmbeddings(newEmbeddings: DataFrame, existingEmbeddings: DataFrame): DataFrame = {
    if (existingEmbeddings.isEmpty) {
      logger.info("No existing embeddings - returning new embeddings only")
      newEmbeddings
    } else {
      logger.info("Merging new embeddings with existing embeddings")
      val mergedCount = existingEmbeddings.count() + newEmbeddings.count()
      val merged = existingEmbeddings.union(newEmbeddings)
      logger.info(s"Merged embeddings: $mergedCount total")
      merged
    }
  }

  def deleteEmbeddingsForChunks(allEmbeddings: DataFrame, chunkIds: Set[String]): DataFrame = {
    if (chunkIds.isEmpty) {
      allEmbeddings
    } else {
      logger.info(s"Deleting embeddings for ${chunkIds.size} chunks")
      allEmbeddings.filter(!col("chunkId").isin(chunkIds.toSeq: _*))
    }
  }
}

// Companion object with static methods for logging from executors
object IncrementalEmbedder extends LazyLogging {

  def logBatch(size: Int): Unit = {
    logger.info(s"Processing batch of $size chunks")
  }

  def logFailure(chunkId: String, error: String): Unit = {
    logger.warn(s"Failed to generate embedding for chunk $chunkId: $error")
  }
}