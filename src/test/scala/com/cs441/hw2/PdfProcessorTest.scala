package com.cs441.hw2

import org.scalatest.funsuite.AnyFunSuite

class PdfProcessorTest extends AnyFunSuite {

  test("cleanText should remove excessive whitespace") {
    val text = "Hello    World   Test"
    val cleaned = PdfProcessor.cleanText(text)

    assert(cleaned == "Hello World Test")
  }

  test("cleanText should trim leading and trailing whitespace") {
    val text = "  Hello World  "
    val cleaned = PdfProcessor.cleanText(text)

    assert(cleaned == "Hello World")
  }

  test("cleanText should handle empty string") {
    val text = ""
    val cleaned = PdfProcessor.cleanText(text)

    assert(cleaned.isEmpty)
  }

  test("cleanText should handle text with only whitespace") {
    val text = "   \t\n   "
    val cleaned = PdfProcessor.cleanText(text)

    assert(cleaned.isEmpty)
  }
}