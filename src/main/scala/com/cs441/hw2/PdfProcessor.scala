package com.cs441.hw2

import org.apache.pdfbox.pdmodel.PDDocument
import org.apache.pdfbox.text.PDFTextStripper
import scala.util.Try

object PdfProcessor {

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