package com.cs441.hw2

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.io.File

/**
 * Manages persistent storage using Parquet.
 */
class StorageLayer(spark: SparkSession, baseOutputDir: String) extends LazyLogging {

  private val documentsPath = s"$baseOutputDir/documents"
  private val chunksPath = s"$baseOutputDir/chunks"
  private val embeddingsPath = s"$baseOutputDir/embeddings"
  private val indexPath = s"$baseOutputDir/retrieval_index"

  def tableExists(path: String): Boolean = {
    val dir = new File(path)
    dir.exists() && dir.isDirectory && dir.listFiles().exists(_.getName.endsWith(".parquet"))
  }

  // ==================== Documents Table ====================

  def saveDocuments(documents: DataFrame): Unit = {
    logger.info(s"Saving ${documents.count()} documents to $documentsPath")

    documents.write
      .mode(SaveMode.Overwrite)
      .parquet(documentsPath)

    logger.info("Documents saved successfully")
  }

  def loadDocuments(): DataFrame = {
    if (tableExists(documentsPath)) {
      logger.info(s"Loading documents from $documentsPath")
      val df = spark.read.parquet(documentsPath)
      logger.info(s"Loaded ${df.count()} documents")
      df
    } else {
      logger.info("No existing documents table found - returning empty DataFrame")
      spark.emptyDataFrame
    }
  }

  // ==================== Chunks Table ====================

  def saveChunks(chunks: DataFrame): Unit = {
    logger.info(s"Saving ${chunks.count()} chunks to $chunksPath")

    chunks.write
      .mode(SaveMode.Overwrite)
      .partitionBy("documentId")
      .parquet(chunksPath)

    logger.info("Chunks saved successfully")
  }

  def loadChunks(): DataFrame = {
    if (tableExists(chunksPath)) {
      logger.info(s"Loading chunks from $chunksPath")
      val df = spark.read.parquet(chunksPath)
      logger.info(s"Loaded ${df.count()} chunks")
      df
    } else {
      logger.info("No existing chunks table found - returning empty DataFrame")
      spark.emptyDataFrame
    }
  }

  // ==================== Embeddings Table ====================

  def saveEmbeddings(embeddings: DataFrame): Unit = {
    logger.info(s"Saving ${embeddings.count()} embeddings to $embeddingsPath")

    embeddings.write
      .mode(SaveMode.Overwrite)
      .parquet(embeddingsPath)

    logger.info("Embeddings saved successfully")
  }

  def loadEmbeddings(): DataFrame = {
    if (tableExists(embeddingsPath)) {
      logger.info(s"Loading embeddings from $embeddingsPath")
      val df = spark.read.parquet(embeddingsPath)
      logger.info(s"Loaded ${df.count()} embeddings")
      df
    } else {
      logger.info("No existing embeddings table found - returning empty DataFrame")
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
}