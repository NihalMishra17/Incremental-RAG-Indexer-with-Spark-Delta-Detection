package com.cs441.hw2

import org.apache.pdfbox.pdmodel.PDDocument
import org.apache.pdfbox.text.PDFTextStripper
import scala.util.Try

/**
 * Handles PDF text extraction using Apache PDFBox
 */
object PdfProcessor {

  /**
   * Extract text from a PDF file
   * @param filePath Path to PDF file
   * @return Try[String] containing extracted text or error
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
   * Extract text from PDF bytes
   * @param bytes PDF file as byte array
   * @return Try[String] containing extracted text or error
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
   * Clean extracted text by normalizing whitespace and removing control characters
   * @param text Raw text to clean
   * @return Cleaned text
   */
  def cleanText(text: String): String = {
    if (text == null || text.isEmpty) {
      ""
    } else {
      text
        .replaceAll("\\s+", " ")  // Normalize whitespace
        .replaceAll("[\\x00-\\x1F\\x7F]", "")  // Remove control characters
        .trim
    }
  }
}