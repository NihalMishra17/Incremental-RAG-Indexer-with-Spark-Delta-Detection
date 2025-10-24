package com.cs441.hw2

import org.scalatest.funsuite.AnyFunSuite

class TextChunkerTest extends AnyFunSuite {

  test("chunk should handle empty text") {
    val chunks = TextChunker.chunk("", 100, 10)
    assert(chunks.isEmpty)
  }

  test("chunk should handle text smaller than chunk size") {
    val text = "Short text"
    val chunks = TextChunker.chunk(text, 100, 10)

    assert(chunks.length == 1)
    assert(chunks.head == text)
  }

  test("chunk should create overlapping chunks") {
    val text = "A" * 250
    val chunks = TextChunker.chunk(text, 100, 20)

    assert(chunks.length > 1)
    chunks.foreach(chunk => assert(chunk.length <= 100))
  }

  test("chunk should be deterministic") {
    val text = "This is a test text that will be chunked multiple times."

    val chunks1 = TextChunker.chunk(text, 20, 5)
    val chunks2 = TextChunker.chunk(text, 20, 5)

    assert(chunks1 == chunks2)
  }

  test("chunk should handle invalid parameters gracefully") {
    val text = "Test text"

    assert(TextChunker.chunk(text, 0, 5).isEmpty)
    assert(TextChunker.chunk(text, -10, 5).isEmpty)
    assert(TextChunker.chunk(text, 100, -5).isEmpty)
  }

  test("chunkWithIndex should return indexed chunks") {
    val text = "A" * 150
    val indexed = TextChunker.chunkWithIndex(text, 50, 10)

    assert(indexed.nonEmpty)
    assert(indexed.head._1 == 0)
  }
}