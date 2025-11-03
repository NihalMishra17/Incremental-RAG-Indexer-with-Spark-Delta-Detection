package com.cs441.hw2

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Represents the delta between current and previous document corpus.
 */
case class DocumentDelta(
                          newDocs: Dataset[Document],
                          changedDocs: Dataset[Document],
                          unchangedDocs: Dataset[Document],
                          deletedDocs: Dataset[Document]
                        )

/**
 * Detects changes in document corpus using content hashing.
 *
 * Design rationale:
 * - Uses Spark anti-joins for efficient set operations
 * - Content hash comparison for change detection (not timestamps)
 * - Four-way classification: new, changed, unchanged, deleted
 */
object DeltaDetector extends LazyLogging {

  /**
   * Detect what changed between current and previous corpus.
   * Returns datasets for each category of change.
   *
   * Note: Uses documentId (not filePath) for matching to handle path variations.
   */
  def detectChanges(
                     currentDocs: Dataset[Document],
                     previousDocs: Dataset[Document]
                   )(implicit spark: SparkSession): DocumentDelta = {
    import spark.implicits._

    logger.info("Detecting document changes...")

    // Edge case: empty corpus
    if (currentDocs.isEmpty) {
      logger.warn("Current corpus is empty - all previous documents are deleted")
      return DocumentDelta(
        newDocs = spark.emptyDataset[Document],
        changedDocs = spark.emptyDataset[Document],
        unchangedDocs = spark.emptyDataset[Document],
        deletedDocs = previousDocs
      )
    }

    if (previousDocs.isEmpty) {
      logger.info("No previous documents - all current documents are new")
      return DocumentDelta(
        newDocs = currentDocs,
        changedDocs = spark.emptyDataset[Document],
        unchangedDocs = spark.emptyDataset[Document],
        deletedDocs = spark.emptyDataset[Document]
      )
    }

    // New documents: in current but not in previous (by documentId, not filePath)
    val newDocs = currentDocs
      .join(previousDocs.select("documentId"), Seq("documentId"), "left_anti")
      .as[Document]

    val newCount = newDocs.count()
    logger.info(s"Found $newCount new documents")

    // Changed documents: same documentId but different contentHash
    val changedDocs = currentDocs.as("current")
      .join(
        previousDocs.as("previous"),
        col("current.documentId") === col("previous.documentId") &&
          col("current.contentHash") =!= col("previous.contentHash"),
        "inner"
      )
      .select(col("current.*"))
      .as[Document]

    val changedCount = changedDocs.count()
    logger.info(s"Found $changedCount changed documents")

    // Unchanged documents: same documentId and same contentHash
    val unchangedDocs = currentDocs.as("current")
      .join(
        previousDocs.as("previous"),
        col("current.documentId") === col("previous.documentId") &&
          col("current.contentHash") === col("previous.contentHash"),
        "inner"
      )
      .select(col("current.*"))
      .as[Document]

    val unchangedCount = unchangedDocs.count()
    logger.info(s"Found $unchangedCount unchanged documents")

    // Deleted documents: in previous but not in current (by documentId)
    val deletedDocs = previousDocs
      .join(currentDocs.select("documentId"), Seq("documentId"), "left_anti")
      .as[Document]

    val deletedCount = deletedDocs.count()
    logger.info(s"Found $deletedCount deleted documents")

    // Summary logging
    logger.info(s"Delta summary: ${previousDocs.count()} previous docs, ${currentDocs.count()} current docs")
    logger.info(s"  New: $newCount, Changed: $changedCount, Unchanged: $unchangedCount, Deleted: $deletedCount")

    DocumentDelta(
      newDocs = newDocs,
      changedDocs = changedDocs,
      unchangedDocs = unchangedDocs,
      deletedDocs = deletedDocs
    )
  }
}