package com.cs441.hw2

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import java.io.File
import java.sql.Timestamp
import java.time.Instant

// Define Document once at package level
case class Document(
                     documentId: String,
                     filePath: String,
                     contentHash: String,
                     rawText: String,
                     ingestionTimestamp: Timestamp,
                     version: Int,
                     status: String
                   )

/**
 * Scans directories for PDF files and creates a DataFrame of documents.
 */
class DocumentScanner(spark: SparkSession) extends LazyLogging {
  import spark.implicits._

  def scanDirectory(inputDir: String): DataFrame = {
    logger.info(s"Scanning directory for PDFs: $inputDir")

    val pdfFiles = findPdfFiles(inputDir)
    logger.info(s"Found ${pdfFiles.size} PDF files")

    if (pdfFiles.isEmpty) {
      logger.warn(s"No PDF files found in $inputDir")
      return spark.emptyDataFrame
    }

    val documentsRDD = spark.sparkContext.parallelize(pdfFiles)
      .map(DocumentScanner.processDocument)
      .filter(_.isDefined)
      .map(_.get)

    val documentsDF = documentsRDD.toDF()
    logger.info(s"Successfully processed ${documentsDF.count()} documents")

    documentsDF
  }

  private def findPdfFiles(dirPath: String): List[String] = {
    val dir = new File(dirPath)

    if (!dir.exists()) {
      logger.error(s"Directory does not exist: $dirPath")
      return List.empty
    }

    if (!dir.isDirectory) {
      logger.error(s"Path is not a directory: $dirPath")
      return List.empty
    }

    def recursiveListFiles(file: File): List[File] = {
      if (file.isDirectory) {
        file.listFiles().toList.flatMap(recursiveListFiles)
      } else if (file.getName.toLowerCase.endsWith(".pdf")) {
        List(file)
      } else {
        List.empty
      }
    }

    recursiveListFiles(dir).map(_.getAbsolutePath)
  }

  def loadDocuments(filePaths: List[String]): DataFrame = {
    logger.info(s"Loading ${filePaths.size} documents")

    val documentsRDD = spark.sparkContext.parallelize(filePaths)
      .map(DocumentScanner.processDocument)
      .filter(_.isDefined)
      .map(_.get)

    documentsRDD.toDF()
  }
}

object DocumentScanner extends LazyLogging {

  def processDocument(filePath: String): Option[Document] = {
    try {
      logger.debug(s"Processing document: $filePath")

      val textResult = PdfProcessor.extractText(filePath)

      textResult match {
        case Right(rawText) =>
          val cleanedText = PdfProcessor.cleanText(rawText)

          if (cleanedText.isEmpty) {
            logger.warn(s"Empty text extracted from $filePath")
            return None
          }

          val documentId = HashUtils.generateDocumentId(filePath)
          val contentHash = HashUtils.sha256(cleanedText)

          val document = Document(
            documentId = documentId,
            filePath = filePath,
            contentHash = contentHash,
            rawText = cleanedText,
            ingestionTimestamp = Timestamp.from(Instant.now()),
            version = 1,
            status = "active"
          )

          logger.debug(s"Successfully processed document: $documentId")
          Some(document)

        case Left(error) =>
          logger.warn(s"Failed to extract text from $filePath: $error")
          None
      }
    } catch {
      case e: Exception =>
        logger.error(s"Exception processing document $filePath: ${e.getMessage}", e)
        None
    }
  }
}