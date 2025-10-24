package com.cs441.hw2

import com.typesafe.scalalogging.LazyLogging
import io.delta.tables.DeltaTable
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.io.File

/**
 * Manages persistent storage using Delta Lake.
 * Provides versioning and ACID transactions.
 */
class StorageLayer(spark: SparkSession, baseOutputDir: String) extends LazyLogging {

  private val documentsPath = s"$baseOutputDir/documents"
  private val chunksPath = s"$baseOutputDir/chunks"
  private val embeddingsPath = s"$baseOutputDir/embeddings"
  private val indexPath = s"$baseOutputDir/retrieval_index"

  def tableExists(path: String): Boolean = {
    try {
      DeltaTable.isDeltaTable(spark, path)
    } catch {
      case _: Exception => false
    }
  }

  // ==================== Documents Table ====================

  def saveDocuments(documents: DataFrame): Unit = {
    logger.info(s"Saving ${documents.count()} documents to Delta table: $documentsPath")

    if (tableExists(documentsPath)) {
      // Append or overwrite existing table
      documents.write
        .format("delta")
        .mode(SaveMode.Overwrite)
        .save(documentsPath)
    } else {
      // Create new Delta table
      documents.write
        .format("delta")
        .mode(SaveMode.Overwrite)
        .save(documentsPath)
    }

    logger.info("Documents saved successfully to Delta Lake")
  }

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

  def getDocumentsVersion(): Long = {
    if (tableExists(documentsPath)) {
      val deltaTable = DeltaTable.forPath(spark, documentsPath)
      deltaTable.history().select("version").first().getLong(0)
    } else {
      -1L
    }
  }

  // ==================== Chunks Table ====================

  def saveChunks(chunks: DataFrame): Unit = {
    logger.info(s"Saving ${chunks.count()} chunks to Delta table: $chunksPath")

    chunks.write
      .format("delta")
      .mode(SaveMode.Overwrite)
      .partitionBy("documentId")
      .save(chunksPath)

    logger.info("Chunks saved successfully to Delta Lake")
  }

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

  def saveEmbeddings(embeddings: DataFrame): Unit = {
    logger.info(s"Saving ${embeddings.count()} embeddings to Delta table: $embeddingsPath")

    embeddings.write
      .format("delta")
      .mode(SaveMode.Overwrite)
      .save(embeddingsPath)

    logger.info("Embeddings saved successfully to Delta Lake")
  }

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

  // ==================== Delta Lake Time Travel ====================

  def loadDocumentsAtVersion(version: Long): DataFrame = {
    logger.info(s"Loading documents at version $version from Delta table")
    spark.read
      .format("delta")
      .option("versionAsOf", version)
      .load(documentsPath)
  }

  def loadChunksAtVersion(version: Long): DataFrame = {
    logger.info(s"Loading chunks at version $version from Delta table")
    spark.read
      .format("delta")
      .option("versionAsOf", version)
      .load(chunksPath)
  }

  def getTableHistory(tablePath: String): DataFrame = {
    if (tableExists(tablePath)) {
      val deltaTable = DeltaTable.forPath(spark, tablePath)
      deltaTable.history()
    } else {
      spark.emptyDataFrame
    }
  }

  // ==================== Index Path ====================

  def getIndexPath(): String = indexPath

  def indexExists(): Boolean = {
    new File(indexPath).exists()
  }

  // ==================== Utility Methods ====================

  def clearAll(): Unit = {
    logger.warn("Clearing all stored data")
    List(documentsPath, chunksPath, embeddingsPath, indexPath).foreach { path =>
      val file = new File(path)
      if (file.exists()) {
        logger.info(s"Deleting $path")
        deleteRecursively(file)
      }
    }
  }

  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      file.listFiles().foreach(deleteRecursively)
    }
    file.delete()
  }

  // ==================== Delta Lake Optimization ====================

  def optimizeTable(tablePath: String): Unit = {
    if (tableExists(tablePath)) {
      logger.info(s"Optimizing Delta table: $tablePath")
      val deltaTable = DeltaTable.forPath(spark, tablePath)
      deltaTable.optimize().executeCompaction()
      logger.info("Table optimization complete")
    }
  }

  def vacuumTable(tablePath: String, retentionHours: Int = 168): Unit = {
    if (tableExists(tablePath)) {
      logger.info(s"Vacuuming Delta table: $tablePath (retention: $retentionHours hours)")
      val deltaTable = DeltaTable.forPath(spark, tablePath)
      deltaTable.vacuum(retentionHours)
      logger.info("Table vacuum complete")
    }
  }
}