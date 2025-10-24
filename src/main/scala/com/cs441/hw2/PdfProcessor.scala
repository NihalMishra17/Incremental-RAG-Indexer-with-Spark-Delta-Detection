package com.cs441.hw2

import com.typesafe.scalalogging.LazyLogging
import org.apache.pdfbox.pdmodel.PDDocument
import org.apache.pdfbox.text.PDFTextStripper

import java.io.File
import scala.util.{Try, Using}

/**
 * Handles PDF text extraction using Apache PDFBox.
 */
object PdfProcessor extends LazyLogging {

  def extractText(filePath: String): Either[String, String] = {
    Try {
      Using.resource(PDDocument.load(new File(filePath))) { document =>
        val stripper = new PDFTextStripper()
        stripper.getText(document)
      }
    }.toEither.left.map { error =>
      logger.warn(s"Failed to extract text from $filePath: ${error.getMessage}")
      s"Error extracting PDF: ${error.getMessage}"
    }
  }

  def cleanText(text: String): String = {
    text
      .replaceAll("\\s+", " ")
      .replaceAll("[\\p{Cntrl}&&[^\n\r\t]]", "")
      .trim
  }
}