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

    logger.info("================================================================================")
    logger.info("INCREMENTAL UPDATE: Detecting changes")
    logger.info("================================================================================")

    logger.info("Step 1: Scanning documents...")
    val currentDocs = DocumentScanner.scanDocuments(spark, config.inputDir)

    logger.info("Step 2: Detecting changes...")
    val detector = new DeltaDetector(spark)
    val delta = detector.detectChanges(currentDocs, existingDocs)

    logger.info(s"Delta stats: new -> ${delta.newDocuments.count()}, " +
      s"changed -> ${delta.changedDocuments.count()}, " +
      s"unchanged -> ${delta.unchangedDocuments.count()}, " +
      s"deleted -> ${delta.deletedDocuments.count()}")

    val deduplicationRatio = if (currentDocs.count() > 0) {
      (delta.unchangedDocuments.count().toDouble / currentDocs.count()) * 100
    } else 0.0

    logger.info(f"Deduplication ratio: $deduplicationRatio%.2f%%")

    val hasChanges = delta.newDocuments.count() > 0 ||
      delta.changedDocuments.count() > 0 ||
      delta.deletedDocuments.count() > 0

    if (!hasChanges) {
      logger.info("No changes detected. Nothing to process.")
      return
    }

    val docsToProcess = delta.newDocuments.union(delta.changedDocuments)
    val docsToProcessCount = docsToProcess.count()

    logger.info(s"Processing $docsToProcessCount changed documents")

    logger.info("Step 3: Chunking changed documents...")
    val newChunks = chunker.chunkDocuments(docsToProcess)

    val existingChunks = storage.loadChunks()
    val changedDocIds = delta.changedDocuments.select("documentId").as[String].collect().toSet
    val allChangedDocIds = changedDocIds ++
      delta.newDocuments.select("documentId").as[String].collect().toSet ++
      delta.deletedDocuments.select("documentId").as[String].collect().toSet

    val mergedChunks = chunker.mergeChunks(newChunks, existingChunks, allChangedDocIds)

    // Force materialization to prevent Spark from recomputing incorrectly
    val materializedChunks = mergedChunks.checkpoint()

    val chunkCount = materializedChunks.count()
    logger.info(s"Total chunks after merge: $chunkCount")

    // Verify what documentIds are in mergedChunks
    logger.info("Document IDs in merged chunks:")
    materializedChunks.select("documentId").distinct().collect().foreach(row =>
      logger.info(s"  - ${row.getString(0)}")
    )

    logger.info("Step 4: Generating embeddings for new chunks...")
    val existingEmbeddingsDF = storage.loadEmbeddings()
    val existingEmbeddings = if (existingEmbeddingsDF.isEmpty) {
      logger.info("No existing embeddings - creating empty dataset")
      spark.emptyDataset[Embedding]
    } else {
      existingEmbeddingsDF.as[Embedding]
    }
    val embeddings = embedder.generateEmbeddings(
      materializedChunks.as[Chunk],
      existingEmbeddings
    )
    val embeddingCount = embeddings.count()
    logger.info(s"Total embeddings: $embeddingCount")

    logger.info("Step 5: Updating storage...")

    val mergedDocs = existingDocs
      .filter(!$"documentId".isin(allChangedDocIds.toSeq: _*))
      .union(docsToProcess)

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