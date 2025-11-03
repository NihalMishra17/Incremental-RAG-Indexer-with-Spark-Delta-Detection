package com.cs441.hw2

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

import java.sql.Timestamp
import java.time.Instant

/**
 * Represents a text chunk with metadata and deterministic ID.
 */
case class Chunk(
                  chunkId: String,
                  documentId: String,
                  chunkIndex: Int,
                  chunkText: String,
                  chunkHash: String,
                  chunkTimestamp: Timestamp,
                  chunkingVersion: String
                )

/**
 * Handles incremental chunking of documents.
 *
 * Design rationale:
 * - Uses Spark window functions for chunk indexing
 * - Deterministic chunk IDs include documentId, index, and content hash
 * - Explode pattern for parallelizing chunk generation
 */
class IncrementalChunker(spark: SparkSession, config: Configuration.ChunkingConfig) extends LazyLogging {
  import spark.implicits._

  /**
   * Chunk documents into fixed-size overlapping segments.
   * Returns DataFrame with deterministic chunk IDs.
   */
  def chunkDocuments(documents: DataFrame): DataFrame = {
    logger.info(s"Chunking documents with chunk size ${config.chunkSize} and overlap ${config.overlap}")

    val documentsCount = documents.count()
    if (documentsCount == 0) {
      logger.info("No documents to chunk")
      return spark.emptyDataFrame
    }

    logger.info(s"Chunking $documentsCount documents")

    // Capture config to avoid serialization issues
    val chunkSize = config.chunkSize
    val overlap = config.overlap
    val version = config.version

    // UDF for chunking text (runs on executors)
    val chunkTextUDF = udf((text: String) => TextChunker.chunk(text, chunkSize, overlap))

    val chunksDF = documents
      .withColumn("chunks", chunkTextUDF(col("content")))
      .withColumn("chunk", explode(col("chunks")))
      .select(
        col("documentId"),
        col("filePath"),
        col("chunk").alias("chunkText")
      )
      // Assign sequential indices within each document
      .withColumn("rowId", monotonically_increasing_id())
      .withColumn("chunkIndex",
        row_number().over(Window.partitionBy("documentId").orderBy("rowId")) - 1
      )
      // Generate deterministic IDs
      .withColumn("chunkHash", sha2(col("chunkText"), 256))
      .withColumn("chunkId",
        concat(
          col("documentId"),
          lit("_"),
          col("chunkIndex"),
          lit("_"),
          substring(col("chunkHash"), 1, 8)
        )
      )
      .withColumn("chunkTimestamp", lit(Timestamp.from(Instant.now())))
      .withColumn("chunkingVersion", lit(version))
      .select(
        col("chunkId"),
        col("documentId"),
        col("chunkIndex"),
        col("chunkText"),
        col("chunkHash"),
        col("chunkTimestamp"),
        col("chunkingVersion")
      )

    val chunksCount = chunksDF.count()
    logger.info(s"Created $chunksCount chunks from $documentsCount documents")

    chunksDF
  }

  /**
   * Merge new chunks with existing, removing stale chunks.
   */
  def mergeChunks(
                   newChunks: DataFrame,
                   existingChunks: DataFrame,
                   changedDocumentIds: Set[String]
                 ): DataFrame = {
    logger.info("Merging new chunks with existing chunks")

    if (existingChunks.isEmpty) {
      logger.info("No existing chunks - returning new chunks only")
      return newChunks
    }

    // Keep only chunks for unchanged documents
    val unchangedChunks = existingChunks
      .filter(!col("documentId").isin(changedDocumentIds.toSeq: _*))

    val unchangedCount = unchangedChunks.count()
    val newCount = newChunks.count()

    logger.info(s"Keeping $unchangedCount unchanged chunks, adding $newCount new chunks")

    if (newCount == 0) {
      logger.info("No new chunks to add - returning unchanged chunks only")
      return unchangedChunks
    }

    unchangedChunks.union(newChunks)
  }

  /**
   * Delete chunks for specified documents.
   */
  def deleteChunksForDocuments(allChunks: DataFrame, documentIds: Set[String]): DataFrame = {
    if (documentIds.isEmpty) {
      allChunks
    } else {
      logger.info(s"Deleting chunks for ${documentIds.size} documents")
      allChunks.filter(!col("documentId").isin(documentIds.toSeq: _*))
    }
  }
}