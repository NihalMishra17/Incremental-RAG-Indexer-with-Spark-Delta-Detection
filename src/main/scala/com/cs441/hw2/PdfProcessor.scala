package com.cs441.hw2

import org.apache.pdfbox.pdmodel.PDDocument
import org.apache.pdfbox.text.PDFTextStripper
import java.io.File
import scala.util.Try

object PdfProcessor {
  def extractText(filePath: String): Try[String] = Try {
    var document: PDDocument = null
    try {
      document = PDDocument.load(new File(filePath))
      val stripper = new PDFTextStripper()
      stripper.getText(document)
    } finally {
      if (document != null) document.close()
    }
  }
}