package com.cs441.hw2

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession

/**
 * Main orchestrator for the incremental delta indexer.
 * Coordinates all components to build and maintain the RAG index.
 */
class SparkDeltaIndexer(config: Configuration.DeltaIndexerConfig) extends LazyLogging {

  private var spark: SparkSession = _
  private var scanner: DocumentScanner = _
  private var detector: DeltaDetector = _
  private var chunker: IncrementalChunker = _
  private var embedder: IncrementalEmbedder = _
  private var storage: StorageLayer = _

  def initialize(): Unit = {
    logger.info("Initializing Spark Delta Indexer")

    // Create Spark session with Delta Lake extensions
    spark = SparkSession.builder()
      .appName(config.spark.appName)
      .master(config.spark.master)
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      // Disable Hadoop shutdown hook to prevent harmless cleanup errors
      .config("spark.hadoop.fs.hdfs.impl.disable.cache", "true")
      .getOrCreate()

    // Also disable the shutdown hook after session creation
    spark.sparkContext.getConf.set("spark.hadoop.util.ShutdownHookManager.enable", "false")

    spark.sparkContext.setLogLevel("WARN")

    // Initialize components
    scanner = new DocumentScanner(spark)
    detector = new DeltaDetector(spark)
    chunker = new IncrementalChunker(spark, config.chunking)
    embedder = new IncrementalEmbedder(spark, config.embedding)
    storage = new StorageLayer(spark, config.outputDir)

    logger.info("Spark Delta Indexer initialized successfully with Delta Lake")
  }

  def runFirstTime(): Unit = {
    logger.info("=" * 80)
    logger.info("FIRST RUN: Processing entire corpus")
    logger.info("=" * 80)

    // Step 1: Scan all documents
    logger.info("Step 1: Scanning documents...")
    val currentDocs = scanner.scanDirectory(config.inputDir)
    val docCount = currentDocs.count()
    logger.info(s"Scanned $docCount documents")

    if (docCount == 0) {
      logger.warn("No documents found. Exiting.")
      return
    }

    // Step 2: Chunk all documents
    logger.info("Step 2: Chunking documents...")
    val chunks = chunker.chunkDocuments(currentDocs)
    val chunkCount = chunks.count()
    logger.info(s"Created $chunkCount chunks")

    // Step 3: Generate embeddings
    logger.info("Step 3: Generating embeddings...")
    val embeddings = embedder.generateEmbeddings(chunks, spark.emptyDataFrame)
    val embeddingCount = embeddings.count()
    logger.info(s"Generated $embeddingCount embeddings")

    // Step 4: Save everything
    logger.info("Step 4: Saving to storage...")
    storage.saveDocuments(currentDocs)
    storage.saveChunks(chunks)
    storage.saveEmbeddings(embeddings)

    logger.info("=" * 80)
    logger.info("FIRST RUN COMPLETE")
    logger.info(s"Documents: $docCount | Chunks: $chunkCount | Embeddings: $embeddingCount")
    logger.info("=" * 80)
  }

  def runIncremental(): Unit = {
    logger.info("=" * 80)
    logger.info("INCREMENTAL RUN: Processing changes only")
    logger.info("=" * 80)

    // Step 1: Scan current documents
    logger.info("Step 1: Scanning current documents...")
    val currentDocs = scanner.scanDirectory(config.inputDir)
    val currentCount = currentDocs.count()
    logger.info(s"Current corpus: $currentCount documents")

    // Step 2: Load previous state
    logger.info("Step 2: Loading previous state...")
    val previousDocs = storage.loadDocuments()
    val previousCount = previousDocs.count()
    logger.info(s"Previous state: $previousCount documents")

    // Step 3: Detect changes
    logger.info("Step 3: Detecting changes...")
    val deltaResult = detector.detectChanges(currentDocs, previousDocs)
    val stats = detector.calculateStats(deltaResult)

    logger.info(s"Delta stats: ${stats.mkString(", ")}")
    val dedup = detector.deduplicationRatio(deltaResult)
    logger.info(f"Deduplication ratio: ${dedup * 100}%.2f%%")

    if (!detector.hasChanges(deltaResult)) {
      logger.info("No changes detected. Nothing to process.")
      logger.info("=" * 80)
      return
    }

    // Step 4: Process changed documents
    logger.info("Step 4: Processing changed documents...")
    val docsToProcess = detector.getDocumentsToProcess(deltaResult)
    val processCount = docsToProcess.count()
    logger.info(s"Processing $processCount changed documents")

    // Step 5: Chunk changed documents
    logger.info("Step 5: Chunking changed documents...")
    val newChunks = chunker.chunkDocuments(docsToProcess)
    val newChunkCount = newChunks.count()
    logger.info(s"Created $newChunkCount new chunks")

    // Step 6: Merge with existing chunks
    logger.info("Step 6: Merging chunks...")
    val existingChunks = storage.loadChunks()
    val changedDocIds = docsToProcess.select("documentId").rdd.map(_.getString(0)).collect().toSet
    val allChunks = chunker.mergeChunks(newChunks, existingChunks, changedDocIds)
    val totalChunks = allChunks.count()
    logger.info(s"Total chunks after merge: $totalChunks")

    // Step 7: Generate embeddings for new chunks
    logger.info("Step 7: Generating embeddings for new chunks...")
    val existingEmbeddings = storage.loadEmbeddings()
    val newEmbeddings = embedder.generateEmbeddings(newChunks, existingEmbeddings)
    val newEmbeddingCount = newEmbeddings.count()
    logger.info(s"Generated $newEmbeddingCount new embeddings")

    // Step 8: Merge embeddings
    logger.info("Step 8: Merging embeddings...")
    val allEmbeddings = embedder.mergeEmbeddings(newEmbeddings, existingEmbeddings)
    val totalEmbeddings = allEmbeddings.count()
    logger.info(s"Total embeddings after merge: $totalEmbeddings")

    // Step 9: Save updated state
    logger.info("Step 9: Saving updated state...")
    storage.saveDocuments(currentDocs)
    storage.saveChunks(allChunks)
    storage.saveEmbeddings(allEmbeddings)

    logger.info("=" * 80)
    logger.info("INCREMENTAL RUN COMPLETE")
    logger.info(s"Processed: $processCount docs | New chunks: $newChunkCount | New embeddings: $newEmbeddingCount")
    logger.info(f"Efficiency: Skipped ${dedup * 100}%.2f%% of corpus")
    logger.info("=" * 80)
  }

  def cleanup(): Unit = {
    if (spark != null) {
      logger.info("Stopping Spark session")
      spark.stop()
    }
  }
}

object SparkDeltaIndexer {
  def main(args: Array[String]): Unit = {
    val config = Configuration.loadOrThrow()
    val indexer = new SparkDeltaIndexer(config)

    try {
      indexer.initialize()

      // Check if this is first run or incremental
      val storage = new StorageLayer(indexer.spark, config.outputDir)
      if (storage.loadDocuments().isEmpty) {
        println("No previous state found. Running first-time indexing...")
        indexer.runFirstTime()
      } else {
        println("Previous state found. Running incremental indexing...")
        indexer.runIncremental()
      }
    } finally {
      indexer.cleanup()
    }
  }
}
