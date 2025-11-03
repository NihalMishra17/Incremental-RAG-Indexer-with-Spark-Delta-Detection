package com.cs441.hw2

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession
import java.time.Instant
import java.time.Duration

/**
 * Main entry point for incremental RAG indexer with Delta Lake.
 *
 * Design rationale:
 * - Singleton pattern with lazy initialization for Spark components
 * - Detects first run vs incremental run automatically
 * - Uses Delta Lake for ACID transactions and versioning
 * - Generates CSV statistics for each run
 * - Checkpoints Spark computations to prevent recomputation
 */
object SparkDeltaIndexer extends LazyLogging {

  // Singleton components initialized once
  private var sparkSession: SparkSession = _
  private var chunker: IncrementalChunker = _
  private var embedder: IncrementalEmbedder = _
  private var storage: StorageLayer = _
  private var config: Configuration.DeltaIndexerConfig = _

  /**
   * Initialize Spark session with Delta Lake extensions and all components.
   */
  def initialize(): Unit = {
    logger.info("Initializing Spark Delta Indexer")

    config = Configuration.loadOrThrow()

    // Create Spark session with Delta Lake support
    sparkSession = SparkSession.builder()
      .appName(config.spark.appName)
      .master(config.spark.master)
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    sparkSession.sparkContext.setCheckpointDir("checkpoints")

    // Initialize pipeline components
    chunker = new IncrementalChunker(sparkSession, config.chunking)
    embedder = new IncrementalEmbedder(sparkSession, config.embedding)
    storage = new StorageLayer(sparkSession, config.outputDir)

    logger.info("Spark Delta Indexer initialized successfully with Delta Lake")
  }

  /**
   * Main execution logic - detects first run vs incremental automatically.
   */
  def run(): Unit = {
    try {
      val existingDocs = storage.loadDocuments()

      // Branch based on whether previous state exists
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

  /**
   * First run: Process entire corpus from scratch.
   * Creates baseline state in Delta Lake.
   */
  private def runFirstTimeIndexing(): Unit = {
    val startTime = Instant.now()
    val spark = sparkSession
    import spark.implicits._

    logger.info("================================================================================")
    logger.info("FIRST RUN: Processing entire corpus")
    logger.info("================================================================================")

    // Step 1: Scan all documents
    logger.info("Step 1: Scanning documents...")
    val currentDocs = DocumentScanner.scanDocuments(spark, config.inputDir)

    val docCount = currentDocs.count()
    if (docCount == 0) {
      logger.warn("No documents found. Exiting.")
      return
    }

    logger.info(s"Scanned $docCount documents")

    // Step 2: Chunk all documents
    logger.info("Step 2: Chunking documents...")
    val chunks = chunker.chunkDocuments(currentDocs)
    val chunkCount = chunks.count()
    logger.info(s"Created $chunkCount chunks")

    // Step 3: Generate embeddings for all chunks
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

    // Step 4: Save to Delta Lake
    logger.info("Step 4: Saving to storage...")
    storage.saveDocuments(currentDocs)
    storage.saveChunks(chunks)
    storage.saveEmbeddings(embeddings.toDF())

    val duration = Duration.between(startTime, Instant.now()).getSeconds

    logger.info("================================================================================")
    logger.info("FIRST RUN COMPLETE")
    logger.info(s"Documents: $docCount | Chunks: $chunkCount | Embeddings: $embeddingCount")
    logger.info(s"Duration: ${duration}s")
    logger.info("================================================================================")

    // Save metrics
    saveRunStats(
      runType = "first_run",
      totalDocs = docCount,
      totalChunks = chunkCount,
      totalEmbeddings = embeddingCount,
      newDocs = docCount,
      durationSec = duration
    )
  }

  /**
   * Incremental run: Process only new/changed documents.
   * Uses delta detection to minimize work.
   */
  private def runIncrementalUpdate(existingDocs: org.apache.spark.sql.DataFrame): Unit = {
    val startTime = Instant.now()
    val spark = sparkSession
    import spark.implicits._
    import org.apache.spark.sql.functions.col

    logger.info("================================================================================")
    logger.info("INCREMENTAL UPDATE: Detecting changes")
    logger.info("================================================================================")

    // Step 1: Scan current documents
    logger.info("Step 1: Scanning documents...")
    val currentDocs = DocumentScanner.scanDocuments(spark, config.inputDir)

    // Step 2: Detect what changed using content hashes
    logger.info("Step 2: Detecting changes...")
    implicit val sparkImplicit: SparkSession = spark

    val currentDocsTyped = if (currentDocs.isEmpty) {
      spark.emptyDataset[Document]
    } else {
      currentDocs.as[Document]
    }

    val delta = DeltaDetector.detectChanges(currentDocsTyped, existingDocs.as[Document])

    val newDocsCount = delta.newDocs.count()
    val changedDocsCount = delta.changedDocs.count()
    val unchangedDocsCount = delta.unchangedDocs.count()
    val deletedDocsCount = delta.deletedDocs.count()

    logger.info(s"Delta stats: new -> $newDocsCount, " +
      s"changed -> $changedDocsCount, " +
      s"unchanged -> $unchangedDocsCount, " +
      s"deleted -> $deletedDocsCount")

    // Calculate efficiency metric
    val deduplicationRatio = if (currentDocs.count() > 0) {
      (unchangedDocsCount.toDouble / currentDocs.count()) * 100
    } else 0.0

    logger.info(f"Deduplication ratio: $deduplicationRatio%.2f%%")

    val hasChanges = newDocsCount > 0 || changedDocsCount > 0 || deletedDocsCount > 0

    // Fast path: No changes detected
    if (!hasChanges) {
      logger.info("No changes detected. Nothing to process.")
      val duration = Duration.between(startTime, Instant.now()).getSeconds

      saveRunStats(
        runType = "incremental_no_changes",
        totalDocs = currentDocs.count(),
        totalChunks = storage.loadChunks().count(),
        totalEmbeddings = storage.loadEmbeddings().count(),
        unchangedDocs = unchangedDocsCount,
        efficiencyPct = 100.0,
        durationSec = duration
      )
      return
    }

    // Process only changed documents
    val docsToProcess = delta.newDocs.union(delta.changedDocs)
    val docsToProcessCount = docsToProcess.count()

    logger.info(s"Processing $docsToProcessCount changed documents")

    // Step 3: Re-chunk only changed documents
    logger.info("Step 3: Chunking changed documents...")
    val newChunks = chunker.chunkDocuments(docsToProcess.toDF())

    val existingChunks = storage.loadChunks()

    // Remove chunks for changed/deleted docs (will be replaced)
    val changedDocIds = delta.changedDocs.select("documentId").as[String].collect().toSet
    val newDocIds = delta.newDocs.select("documentId").as[String].collect().toSet
    val deletedDocIds = delta.deletedDocs.select("documentId").as[String].collect().toSet
    val docsToRemoveChunks = changedDocIds ++ newDocIds ++ deletedDocIds

    logger.info(s"Removing chunks for ${docsToRemoveChunks.size} changed/new/deleted docs")

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

    // Merge unchanged and new chunks
    val mergedChunks = if (newCount == 0) {
      logger.info("No new chunks - using only unchanged chunks")
      unchangedChunks
    } else {
      unchangedChunks.union(newChunks)
    }

    // Checkpoint to prevent Spark from recomputing incorrectly
    val materializedChunks = mergedChunks.checkpoint()

    val chunkCount = materializedChunks.count()
    logger.info(s"Total chunks after merge: $chunkCount")

    if (chunkCount > 0) {
      logger.info("Document IDs in merged chunks:")
      materializedChunks.select("documentId").distinct().collect().foreach(row =>
        logger.info(s"  - ${row.getString(0)}")
      )
    } else {
      logger.info("No chunks remaining after merge")
    }

    // Step 4: Generate embeddings only for new chunks
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

    // Step 5: Atomic update of Delta Lake tables
    logger.info("Step 5: Updating storage...")

    val allChangedDocIds = changedDocIds ++ newDocIds ++ deletedDocIds

    val mergedDocs = existingDocs
      .filter(!$"documentId".isin(allChangedDocIds.toSeq: _*))
      .union(docsToProcess.toDF())

    val finalDocCount = mergedDocs.count()
    val finalChunkCount = chunkCount
    val finalEmbeddingCount = embeddingCount

    storage.saveDocuments(mergedDocs)
    storage.saveChunks(materializedChunks)
    storage.saveEmbeddings(embeddings.toDF())

    val duration = Duration.between(startTime, Instant.now()).getSeconds

    logger.info("================================================================================")
    logger.info("INCREMENTAL UPDATE COMPLETE")
    logger.info(f"Efficiency: Skipped $deduplicationRatio%.2f%% of corpus")
    logger.info(s"Total: $finalDocCount docs | $finalChunkCount chunks | $finalEmbeddingCount embeddings")
    logger.info(s"Duration: ${duration}s")
    logger.info("================================================================================")

    // Save metrics
    saveRunStats(
      runType = "incremental_update",
      totalDocs = finalDocCount,
      totalChunks = finalChunkCount,
      totalEmbeddings = finalEmbeddingCount,
      newDocs = newDocsCount,
      changedDocs = changedDocsCount,
      unchangedDocs = unchangedDocsCount,
      deletedDocs = deletedDocsCount,
      efficiencyPct = deduplicationRatio,
      durationSec = duration
    )
  }

  /**
   * Save run statistics to CSV for tracking efficiency and cost.
   * Uses vertical format (metrics as rows) for readability.
   */
  private def saveRunStats(
                            runType: String,
                            totalDocs: Long,
                            totalChunks: Long,
                            totalEmbeddings: Long,
                            newDocs: Long = 0,
                            changedDocs: Long = 0,
                            unchangedDocs: Long = 0,
                            deletedDocs: Long = 0,
                            efficiencyPct: Double = 0.0,
                            durationSec: Long
                          ): Unit = {
    try {
      val spark = sparkSession
      import spark.implicits._

      val timestamp = Instant.now().toString.replace(":", "-").replace(".", "-")
      val statsPath = s"${config.outputDir}/run-stats-$timestamp.csv"

      // Vertical CSV format: metric, value
      val statsData = Seq(
        ("metric", "value"),
        ("timestamp", timestamp),
        ("run_type", runType),
        ("environment", config.spark.master),
        ("input_dir", config.inputDir),
        ("output_dir", config.outputDir),
        ("model", s"${config.embedding.model}-${config.embedding.version}"),
        ("chunk_size", config.chunking.chunkSize.toString),
        ("chunk_overlap", config.chunking.overlap.toString),
        ("total_documents", totalDocs.toString),
        ("total_chunks", totalChunks.toString),
        ("total_embeddings", totalEmbeddings.toString),
        ("new_documents", newDocs.toString),
        ("changed_documents", changedDocs.toString),
        ("unchanged_documents", unchangedDocs.toString),
        ("deleted_documents", deletedDocs.toString),
        ("efficiency_pct", f"$efficiencyPct%.2f"),
        ("duration_seconds", durationSec.toString)
      )

      // Write directly to S3A (Spark handles filesystem correctly)
      val df = statsData.toDF("metric", "value")

      df.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(statsPath)

      logger.info(s"✓ Run statistics saved to: $statsPath")

    } catch {
      case e: Exception =>
        logger.error(s"Failed to save run statistics: ${e.getMessage}", e)
      // Don't fail the job if stats saving fails
    }
  }

  /**
   * Clean shutdown of Spark session.
   */
  def cleanup(): Unit = {
    if (sparkSession != null) {
      logger.info("Stopping Spark session")
      sparkSession.stop()
    }
  }

  /**
   * Main entry point.
   */
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