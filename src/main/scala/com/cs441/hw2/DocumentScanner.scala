package com.cs441.hw2

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SparkSession}
import java.security.MessageDigest

object DocumentScanner extends LazyLogging {

  def scanDocuments(spark: SparkSession, inputDir: String): DataFrame = {
    import spark.implicits._

    logger.info(s"Scanning directory for PDFs: $inputDir")

    try {
      val filesDF = spark.read
        .format("binaryFile")
        .option("pathGlobFilter", "*.pdf")
        .load(inputDir)

      val fileCount = filesDF.count()
      logger.info(s"Found $fileCount PDF files")

      if (fileCount == 0) {
        logger.warn(s"No PDF files found in: $inputDir")
        return spark.emptyDataFrame
      }

      val documentsRDD = filesDF.rdd.flatMap { row =>
        val path = row.getAs[String]("path")
        val content = row.getAs[Array[Byte]]("content")

        PdfProcessor.extractTextFromBytes(content) match {
          case scala.util.Success(text) =>
            try {
              val fileName = path.split("/").last
              val documentId = fileName.replaceAll("\\.pdf$", "")
              val contentHash = computeHash(text)

              Some(Document(
                documentId = documentId,
                filePath = path,
                fileName = fileName,
                content = text,
                contentHash = contentHash,
                processedAt = System.currentTimeMillis(),
                version = "v1"
              ))
            } catch {
              case e: Exception =>
                logger.error(s"Error processing $path: ${e.getMessage}", e)
                None
            }

          case scala.util.Failure(error) =>
            logger.error(s"Failed to extract text from $path: ${error.getMessage}")
            None
        }
      }

      val documents = documentsRDD.collect().toSeq
      logger.info(s"Successfully processed ${documents.length} documents")

      spark.createDataset(documents).toDF()

    } catch {
      case e: Exception =>
        logger.error(s"Error scanning directory $inputDir: ${e.getMessage}", e)
        spark.emptyDataFrame
    }
  }

  def computeHash(content: String): String = {
    val digest = MessageDigest.getInstance("SHA-256")
    val hashBytes = digest.digest(content.getBytes("UTF-8"))
    hashBytes.map("%02x".format(_)).mkString
  }
}

case class Document(
                     documentId: String,
                     filePath: String,
                     fileName: String,
                     content: String,
                     contentHash: String,
                     processedAt: Long,
                     version: String
                   )