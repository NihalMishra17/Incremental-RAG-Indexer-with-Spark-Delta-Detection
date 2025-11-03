package com.cs441.hw2

import com.typesafe.scalalogging.LazyLogging
import io.delta.tables.DeltaTable
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.hadoop.fs.{FileSystem, Path}

/**
 * Delta Lake storage layer for documents, chunks, and embeddings.
 *
 * Design rationale:
 * - Uses Delta Lake for ACID transactions and versioning
 * - Separate tables for normalized data model
 * - Verifies S3 writes for debugging
 * - Partitions chunks by documentId for efficient updates
 */
class StorageLayer(spark: SparkSession, baseOutputDir: String) extends LazyLogging {

  private val documentsPath = s"$baseOutputDir/documents"
  private val chunksPath = s"$baseOutputDir/chunks"
  private val embeddingsPath = s"$baseOutputDir/embeddings"
  private val indexPath = s"$baseOutputDir/retrieval_index"

  logger.info(s"StorageLayer initialized with base path: $baseOutputDir")
  logger.info(s"  Documents: $documentsPath")
  logger.info(s"  Chunks: $chunksPath")
  logger.info(s"  Embeddings: $embeddingsPath")

  /**
   * Check if Delta table exists at path.
   */
  def tableExists(path: String): Boolean = {
    try {
      DeltaTable.isDeltaTable(spark, path)
    } catch {
      case _: Exception => false
    }
  }

  /**
   * Verify S3 write succeeded by checking filesystem.
   */
  private def verifyS3Write(path: String): Unit = {
    try {
      val hadoopPath = new Path(path)
      val fs = hadoopPath.getFileSystem(spark.sparkContext.hadoopConfiguration)

      if (fs.exists(hadoopPath)) {
        val status = fs.listStatus(hadoopPath)
        logger.info(s"✓ Verified write to $path - ${status.length} files/dirs found")

        // Check for Delta log
        val deltaLog = new Path(path, "_delta_log")
        if (fs.exists(deltaLog)) {
          logger.info(s"✓ Delta log exists at $deltaLog")
        } else {
          logger.warn(s"✗ No Delta log found at $deltaLog")
        }
      } else {
        logger.error(s"✗ Path does not exist after write: $path")
      }
    } catch {
      case e: Exception =>
        logger.error(s"Error verifying S3 write to $path: ${e.getMessage}")
    }
  }

  // ==================== Documents Table ====================

  /**
   * Save documents to Delta table (overwrite mode).
   */
  def saveDocuments(documents: DataFrame): Unit = {
    val count = documents.count()
    logger.info(s"Saving $count documents to Delta table: $documentsPath")

    if (count == 0) {
      logger.warn("DataFrame is empty - nothing to save!")
      return
    }

    documents.write
      .format("delta")
      .mode(SaveMode.Overwrite)
      .save(documentsPath)

    logger.info("Documents write command completed")
    verifyS3Write(documentsPath)
  }

  /**
   * Load documents from Delta table.
   */
  def loadDocuments(): DataFrame = {
    if (tableExists(documentsPath)) {
      logger.info(s"Loading documents from Delta table: $documentsPath")
      val df = spark.read.format("delta").load(documentsPath)
      logger.info(s"Loaded ${df.count()} documents from Delta Lake")
      df
    } else {
      logger.info("No existing documents table found - returning empty DataFrame")
      spark.emptyDataFrame
    }
  }

  // ==================== Chunks Table ====================

  /**
   * Save chunks to Delta table with partitioning by documentId.
   */
  def saveChunks(chunks: DataFrame): Unit = {
    logger.info(s"saveChunks called with DataFrame")

    val count = chunks.count()

    if (count == 0) {
      logger.info("No chunks to save - DataFrame is empty")
      return
    }

    val distinctDocs = chunks.select("documentId").distinct().collect().map(_.getString(0))
    logger.info(s"Document IDs in chunks to save: ${distinctDocs.mkString(", ")}")
    logger.info(s"Saving $count chunks to Delta table: $chunksPath")

    if (tableExists(chunksPath)) {
      // Overwrite with dynamic partitioning for incremental updates
      logger.info("Merging chunks into existing table")
      chunks.write
        .format("delta")
        .mode(SaveMode.Overwrite)
        .option("overwriteSchema", "false")
        .option("partitionOverwriteMode", "dynamic")
        .partitionBy("documentId")
        .save(chunksPath)
    } else {
      // First time - create partitioned table
      logger.info("Creating new chunks table")
      chunks.write
        .format("delta")
        .mode(SaveMode.Overwrite)
        .partitionBy("documentId")
        .save(chunksPath)
    }

    logger.info("Chunks write command completed")
    verifyS3Write(chunksPath)
  }

  /**
   * Load chunks from Delta table.
   */
  def loadChunks(): DataFrame = {
    if (tableExists(chunksPath)) {
      logger.info(s"Loading chunks from Delta table: $chunksPath")
      val df = spark.read.format("delta").load(chunksPath)
      logger.info(s"Loaded ${df.count()} chunks from Delta Lake")
      df
    } else {
      logger.info("No existing chunks table found - returning empty DataFrame")
      spark.emptyDataFrame
    }
  }

  // ==================== Embeddings Table ====================

  /**
   * Save embeddings to Delta table (overwrite mode).
   */
  def saveEmbeddings(embeddings: DataFrame): Unit = {
    val count = embeddings.count()
    logger.info(s"Saving $count embeddings to Delta table: $embeddingsPath")

    if (count == 0) {
      logger.warn("DataFrame is empty - nothing to save!")
      return
    }

    embeddings.write
      .format("delta")
      .mode(SaveMode.Overwrite)
      .save(embeddingsPath)

    logger.info("Embeddings write command completed")
    verifyS3Write(embeddingsPath)
  }

  /**
   * Load embeddings from Delta table.
   */
  def loadEmbeddings(): DataFrame = {
    if (tableExists(embeddingsPath)) {
      logger.info(s"Loading embeddings from Delta table: $embeddingsPath")
      val df = spark.read.format("delta").load(embeddingsPath)
      logger.info(s"Loaded ${df.count()} embeddings from Delta Lake")
      df
    } else {
      logger.info("No existing embeddings table found - returning empty DataFrame")
      spark.emptyDataFrame
    }
  }

  // ==================== Index Path ====================

  def getIndexPath(): String = indexPath

  def indexExists(): Boolean = {
    try {
      val path = new Path(indexPath)
      val fs = path.getFileSystem(spark.sparkContext.hadoopConfiguration)
      fs.exists(path)
    } catch {
      case _: Exception => false
    }
  }
}