package com.cs441.hw2

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession

object SparkDeltaIndexer extends LazyLogging {

  private var sparkSession: SparkSession = _
  private var chunker: IncrementalChunker = _
  private var embedder: IncrementalEmbedder = _
  private var storage: StorageLayer = _
  private var config: Configuration.DeltaIndexerConfig = _

  def initialize(): Unit = {
    logger.info("Initializing Spark Delta Indexer")

    config = Configuration.loadOrThrow()

    sparkSession = SparkSession.builder()
      .appName(config.spark.appName)
      .master(config.spark.master)
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    // Set checkpoint directory
    sparkSession.sparkContext.setCheckpointDir("checkpoints")

    chunker = new IncrementalChunker(sparkSession, config.chunking)
    embedder = new IncrementalEmbedder(sparkSession, config.embedding)
    storage = new StorageLayer(sparkSession, config.outputDir)

    logger.info("Spark Delta Indexer initialized successfully with Delta Lake")
  }

  def run(): Unit = {
    try {
      val existingDocs = storage.loadDocuments()

      if (existingDocs.isEmpty) {
        logger.info("No previous state found. Running first-time indexing...")
        runFirstTimeIndexing()
      } else {
        logger.info("Previous state found. Running incremental update...")
        runIncrementalUpdate(existingDocs)
      }

    } catch {
      case e: Exception =>
        logger.error(s"Error during indexing: ${e.getMessage}", e)
        throw e
    }
  }

  private def runFirstTimeIndexing(): Unit = {
    val spark = sparkSession
    import spark.implicits._

    logger.info("================================================================================")
    logger.info("FIRST RUN: Processing entire corpus")
    logger.info("================================================================================")

    logger.info("Step 1: Scanning documents...")
    val currentDocs = DocumentScanner.scanDocuments(spark, config.inputDir)

    val docCount = currentDocs.count()
    if (docCount == 0) {
      logger.warn("No documents found. Exiting.")
      return
    }

    logger.info(s"Scanned $docCount documents")

    logger.info("Step 2: Chunking documents...")
    val chunks = chunker.chunkDocuments(currentDocs)
    val chunkCount = chunks.count()
    logger.info(s"Created $chunkCount chunks")

    logger.info("Step 3: Generating embeddings...")
    val existingEmbeddingsDF = storage.loadEmbeddings()
    val existingEmbeddings = if (existingEmbeddingsDF.isEmpty) {
      logger.info("No existing embeddings - creating empty dataset")
      spark.emptyDataset[Embedding]
    } else {
      existingEmbeddingsDF.as[Embedding]
    }
    val embeddings = embedder.generateEmbeddings(
      chunks.as[Chunk],
      existingEmbeddings
    )
    val embeddingCount = embeddings.count()
    logger.info(s"Generated $embeddingCount embeddings")

    logger.info("Step 4: Saving to storage...")
    storage.saveDocuments(currentDocs)
    storage.saveChunks(chunks)
    storage.saveEmbeddings(embeddings.toDF())

    logger.info("================================================================================")
    logger.info("FIRST RUN COMPLETE")
    logger.info(s"Documents: $docCount | Chunks: $chunkCount | Embeddings: $embeddingCount")
    logger.info("================================================================================")
  }

  private def runIncrementalUpdate(existingDocs: org.apache.spark.sql.DataFrame): Unit = {
    val spark = sparkSession
    import spark.implicits._
    import org.apache.spark.sql.functions.col

    logger.info("================================================================================")
    logger.info("INCREMENTAL UPDATE: Detecting changes")
    logger.info("================================================================================")

    logger.info("Step 1: Scanning documents...")
    val currentDocs = DocumentScanner.scanDocuments(spark, config.inputDir)

    logger.info("Step 2: Detecting changes...")
    implicit val sparkImplicit: SparkSession = spark

    // Convert to Dataset[Document] if not empty, otherwise use empty dataset
    val currentDocsTyped = if (currentDocs.isEmpty) {
      spark.emptyDataset[Document]
    } else {
      currentDocs.as[Document]
    }

    val delta = DeltaDetector.detectChanges(currentDocsTyped, existingDocs.as[Document])

    logger.info(s"Delta stats: new -> ${delta.newDocs.count()}, " +
      s"changed -> ${delta.changedDocs.count()}, " +
      s"unchanged -> ${delta.unchangedDocs.count()}, " +
      s"deleted -> ${delta.deletedDocs.count()}")

    val deduplicationRatio = if (currentDocs.count() > 0) {
      (delta.unchangedDocs.count().toDouble / currentDocs.count()) * 100
    } else 0.0

    logger.info(f"Deduplication ratio: $deduplicationRatio%.2f%%")

    val hasChanges = delta.newDocs.count() > 0 ||
      delta.changedDocs.count() > 0 ||
      delta.deletedDocs.count() > 0

    if (!hasChanges) {
      logger.info("No changes detected. Nothing to process.")
      return
    }

    val docsToProcess = delta.newDocs.union(delta.changedDocs)
    val docsToProcessCount = docsToProcess.count()

    logger.info(s"Processing $docsToProcessCount changed documents")

    logger.info("Step 3: Chunking changed documents...")
    val newChunks = chunker.chunkDocuments(docsToProcess.toDF())

    val existingChunks = storage.loadChunks()

    // Remove chunks for: changed docs (will be replaced), new docs (in case of ID collision), deleted docs (no longer exist)
    val changedDocIds = delta.changedDocs.select("documentId").as[String].collect().toSet
    val newDocIds = delta.newDocs.select("documentId").as[String].collect().toSet
    val deletedDocIds = delta.deletedDocs.select("documentId").as[String].collect().toSet
    val docsToRemoveChunks = changedDocIds ++ newDocIds ++ deletedDocIds

    logger.info(s"Removing chunks for ${docsToRemoveChunks.size} changed/new/deleted docs")

    // If all current docs are gone (empty corpus), return empty chunks
    val unchangedChunks = if (existingChunks.isEmpty || currentDocsTyped.isEmpty) {
      if (currentDocsTyped.isEmpty) {
        logger.info("Corpus is empty - removing all chunks")
        spark.emptyDataFrame
      } else {
        existingChunks
      }
    } else if (docsToRemoveChunks.isEmpty) {
      existingChunks
    } else {
      existingChunks.filter(!col("documentId").isin(docsToRemoveChunks.toSeq: _*))
    }

    val unchangedCount = unchangedChunks.count()
    val newCount = newChunks.count()
    logger.info(s"Keeping $unchangedCount unchanged chunks, adding $newCount new chunks")

    val mergedChunks = if (newCount == 0) {
      logger.info("No new chunks - using only unchanged chunks")
      unchangedChunks
    } else {
      unchangedChunks.union(newChunks)
    }

    // Force materialization to prevent Spark from recomputing incorrectly
    val materializedChunks = mergedChunks.checkpoint()

    val chunkCount = materializedChunks.count()
    logger.info(s"Total chunks after merge: $chunkCount")

    // Verify what documentIds are in mergedChunks (only if not empty)
    if (chunkCount > 0) {
      logger.info("Document IDs in merged chunks:")
      materializedChunks.select("documentId").distinct().collect().foreach(row =>
        logger.info(s"  - ${row.getString(0)}")
      )
    } else {
      logger.info("No chunks remaining after merge")
    }

    logger.info("Step 4: Generating embeddings for new chunks...")
    val existingEmbeddingsDF = storage.loadEmbeddings()
    val existingEmbeddings = if (existingEmbeddingsDF.isEmpty) {
      logger.info("No existing embeddings - creating empty dataset")
      spark.emptyDataset[Embedding]
    } else {
      existingEmbeddingsDF.as[Embedding]
    }

    val embeddings = if (chunkCount == 0) {
      logger.info("No chunks to embed - using empty embeddings dataset")
      spark.emptyDataset[Embedding]
    } else {
      embedder.generateEmbeddings(
        materializedChunks.as[Chunk],
        existingEmbeddings
      )
    }

    val embeddingCount = embeddings.count()
    logger.info(s"Total embeddings: $embeddingCount")

    logger.info("Step 5: Updating storage...")

    // Reuse the doc IDs we already collected above
    val allChangedDocIds = changedDocIds ++ newDocIds ++ deletedDocIds

    val mergedDocs = existingDocs
      .filter(!$"documentId".isin(allChangedDocIds.toSeq: _*))
      .union(docsToProcess.toDF())

    // Cache counts before saving
    val finalDocCount = mergedDocs.count()
    val finalChunkCount = chunkCount
    val finalEmbeddingCount = embeddingCount

    storage.saveDocuments(mergedDocs)
    storage.saveChunks(materializedChunks)
    storage.saveEmbeddings(embeddings.toDF())

    logger.info("================================================================================")
    logger.info("INCREMENTAL UPDATE COMPLETE")
    logger.info(f"Efficiency: Skipped $deduplicationRatio%.2f%% of corpus")
    logger.info(s"Total: $finalDocCount docs | $finalChunkCount chunks | $finalEmbeddingCount embeddings")
    logger.info("================================================================================")
  }

  def cleanup(): Unit = {
    if (sparkSession != null) {
      logger.info("Stopping Spark session")
      sparkSession.stop()
    }
  }

  def main(args: Array[String]): Unit = {
    try {
      initialize()
      run()
    } catch {
      case e: Exception =>
        logger.error("Fatal error in Spark Delta Indexer", e)
        sys.exit(1)
    } finally {
      cleanup()
    }
  }
}