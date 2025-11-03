package com.cs441.hw2

import org.apache.pdfbox.pdmodel.PDDocument
import org.apache.pdfbox.text.PDFTextStripper
import scala.util.Try

/**
 * PDF text extraction using Apache PDFBox.
 *
 * Design rationale:
 * - Returns Try for functional error handling
 * - Ensures resources are closed with try-finally
 * - Supports both file path and byte array inputs
 */
object PdfProcessor {

  /**
   * Extract text from PDF file path.
   */
  def extractText(filePath: String): Try[String] = {
    Try {
      val document = PDDocument.load(new java.io.File(filePath))
      try {
        val stripper = new PDFTextStripper()
        stripper.getText(document)
      } finally {
        document.close()
      }
    }
  }

  /**
   * Extract text from PDF bytes (for Spark binaryFile format).
   */
  def extractTextFromBytes(bytes: Array[Byte]): Try[String] = {
    Try {
      val document = PDDocument.load(bytes)
      try {
        val stripper = new PDFTextStripper()
        stripper.getText(document)
      } finally {
        document.close()
      }
    }
  }

  /**
   * Clean extracted text by normalizing whitespace.
   */
  def cleanText(text: String): String = {
    if (text == null || text.isEmpty) {
      ""
    } else {
      text
        .replaceAll("\\s+", " ")
        .replaceAll("[\\x00-\\x1F\\x7F]", "")
        .trim
    }
  }
}