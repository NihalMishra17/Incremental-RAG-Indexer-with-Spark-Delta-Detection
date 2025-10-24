package com.cs441.hw2

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Result of delta detection
 */
case class DeltaResult(
                        newDocuments: DataFrame,
                        changedDocuments: DataFrame,
                        unchangedDocuments: DataFrame,
                        deletedDocuments: DataFrame
                      )

/**
 * Detects changes between current and previous document snapshots.
 * Uses Spark anti-joins for efficient delta detection.
 */
class DeltaDetector(spark: SparkSession) extends LazyLogging {
  import spark.implicits._

  def detectChanges(currentDocs: DataFrame, previousDocs: DataFrame): DeltaResult = {
    logger.info("Detecting document changes...")

    if (previousDocs.isEmpty) {
      logger.info("No previous documents found - treating all as new")
      return DeltaResult(
        newDocuments = currentDocs,
        changedDocuments = spark.emptyDataFrame,
        unchangedDocuments = spark.emptyDataFrame,
        deletedDocuments = spark.emptyDataFrame
      )
    }

    val current = currentDocs.alias("current")
    val previous = previousDocs.alias("previous")

    // New documents (in current but not in previous)
    val newDocs = current
      .join(previous, col("current.filePath") === col("previous.filePath"), "left_anti")

    val newCount = newDocs.count()
    logger.info(s"Found $newCount new documents")

    // Changed documents (same filePath but different contentHash)
    val changedDocs = current
      .join(
        previous,
        col("current.filePath") === col("previous.filePath") &&
          col("current.contentHash") =!= col("previous.contentHash"),
        "inner"
      )
      .select(col("current.*"))

    val changedCount = changedDocs.count()
    logger.info(s"Found $changedCount changed documents")

    // Unchanged documents (same filePath and same contentHash)
    val unchangedDocs = current
      .join(
        previous,
        col("current.filePath") === col("previous.filePath") &&
          col("current.contentHash") === col("previous.contentHash"),
        "inner"
      )
      .select(col("current.*"))

    val unchangedCount = unchangedDocs.count()
    logger.info(s"Found $unchangedCount unchanged documents")

    // Deleted documents (in previous but not in current)
    val deletedDocs = previous
      .join(current, col("previous.filePath") === col("current.filePath"), "left_anti")
      .withColumn("status", lit("deleted"))

    val deletedCount = deletedDocs.count()
    logger.info(s"Found $deletedCount deleted documents")

    val totalCurrent = currentDocs.count()
    val totalPrevious = previousDocs.count()
    logger.info(s"Delta summary: $totalPrevious previous docs, $totalCurrent current docs")
    logger.info(s"  New: $newCount, Changed: $changedCount, Unchanged: $unchangedCount, Deleted: $deletedCount")

    DeltaResult(
      newDocuments = newDocs,
      changedDocuments = changedDocs,
      unchangedDocuments = unchangedDocs,
      deletedDocuments = deletedDocs
    )
  }

  def getDocumentsToProcess(deltaResult: DeltaResult): DataFrame = {
    val toProcess = deltaResult.newDocuments.union(deltaResult.changedDocuments)
    val count = toProcess.count()
    logger.info(s"Documents to process: $count")
    toProcess
  }

  def calculateStats(deltaResult: DeltaResult): Map[String, Long] = {
    Map(
      "new" -> deltaResult.newDocuments.count(),
      "changed" -> deltaResult.changedDocuments.count(),
      "unchanged" -> deltaResult.unchangedDocuments.count(),
      "deleted" -> deltaResult.deletedDocuments.count()
    )
  }

  def hasChanges(deltaResult: DeltaResult): Boolean = {
    !deltaResult.newDocuments.isEmpty || !deltaResult.changedDocuments.isEmpty
  }

  def deduplicationRatio(deltaResult: DeltaResult): Double = {
    val stats = calculateStats(deltaResult)
    val total = stats.values.sum
    if (total == 0) 0.0
    else stats("unchanged").toDouble / total.toDouble
  }
}