package com.cs441.hw2

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{Dataset, SparkSession}
import java.io.File
import java.security.MessageDigest
import scala.util.{Try, Success, Failure}

/**
 * Scans directory for PDF documents and extracts metadata
 */
class DocumentScanner(spark: SparkSession) extends LazyLogging {

  /**
   * Scans a directory for PDF files and processes them
   * @param directoryPath Path to directory containing PDFs
   * @return Dataset of Document objects
   */
  def scanDirectory(directoryPath: String): Dataset[Document] = {
    import spark.implicits._

    logger.info(s"Scanning directory for PDFs: $directoryPath")

    val directory = new File(directoryPath)

    if (!directory.exists()) {
      logger.error(s"Directory does not exist: $directoryPath")
      return spark.emptyDataset[Document]
    }

    if (!directory.isDirectory) {
      logger.error(s"Path is not a directory: $directoryPath")
      return spark.emptyDataset[Document]
    }

    val pdfFiles = directory.listFiles().filter { file =>
      file.isFile && file.getName.toLowerCase.endsWith(".pdf")
    }

    if (pdfFiles.isEmpty) {
      logger.warn(s"No PDF files found in directory: $directoryPath")
      return spark.emptyDataset[Document]
    }

    logger.info(s"Found ${pdfFiles.length} PDF files")

    // Process PDFs
    val documents = pdfFiles.flatMap(processDocument).toSeq

    logger.info(s"Successfully processed ${documents.length} documents")

    spark.createDataset(documents)
  }

  /**
   * Process a single PDF file
   * @param file PDF file to process
   * @return Option[Document]
   */
  private def processDocument(file: File): Option[Document] = {
    logger.debug(s"Processing document: ${file.getName}")

    PdfProcessor.extractText(file.getAbsolutePath) match {
      case Success(rawText) =>
        val cleanedText = cleanText(rawText)
        val contentHash = computeHash(cleanedText)

        Some(Document(
          documentId = file.getName,
          filePath = file.getAbsolutePath,
          fileName = file.getName,
          content = cleanedText,
          contentHash = contentHash,
          processedAt = System.currentTimeMillis(),
          version = "v1"
        ))

      case Failure(error) =>
        logger.error(s"Failed to extract text from ${file.getAbsolutePath}: ${error.getMessage}")
        None
    }
  }

  /**
   * Clean extracted text
   * @param text Raw text from PDF
   * @return Cleaned text
   */
  private def cleanText(text: String): String = {
    text
      .replaceAll("\\s+", " ")  // Normalize whitespace
      .replaceAll("[\\x00-\\x1F\\x7F]", "")  // Remove control characters
      .trim
  }

  /**
   * Compute SHA-256 hash of content
   * @param content Content to hash
   * @return Hex string of hash
   */
  def computeHash(content: String): String = {
    val digest = MessageDigest.getInstance("SHA-256")
    val hashBytes = digest.digest(content.getBytes("UTF-8"))
    hashBytes.map("%02x".format(_)).mkString
  }
}

/**
 * Document case class representing a processed PDF
 */
case class Document(
                     documentId: String,
                     filePath: String,
                     fileName: String,
                     content: String,
                     contentHash: String,
                     processedAt: Long,
                     version: String
                   )