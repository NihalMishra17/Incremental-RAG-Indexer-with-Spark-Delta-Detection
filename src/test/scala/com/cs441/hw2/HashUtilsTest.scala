package com.cs441.hw2

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class HashUtilsTest extends AnyFunSuite with Matchers {

  test("sha256 should produce consistent hashes") {
    val text = "Hello, World!"
    val hash1 = HashUtils.sha256(text)
    val hash2 = HashUtils.sha256(text)

    assert(hash1 == hash2)
    assert(hash1.length == 64)
  }

  test("sha256 should produce different hashes for different content") {
    val text1 = "Hello, World!"
    val text2 = "Hello, World"

    val hash1 = HashUtils.sha256(text1)
    val hash2 = HashUtils.sha256(text2)

    assert(hash1 != hash2)
  }

  test("generateChunkId should be deterministic") {
    val docId = "doc_123"
    val chunkIndex = 0
    val content = "This is a test chunk"

    val id1 = HashUtils.generateChunkId(docId, chunkIndex, content)
    val id2 = HashUtils.generateChunkId(docId, chunkIndex, content)

    assert(id1 == id2)
    assert(id1.startsWith(s"${docId}_${chunkIndex}_"))
  }

  test("generateDocumentId should normalize path separators") {
    val unixPath = "/path/to/document.pdf"
    val windowsPath = "\\path\\to\\document.pdf"

    val id1 = HashUtils.generateDocumentId(unixPath)
    val id2 = HashUtils.generateDocumentId(windowsPath)

    assert(id1 == id2)
  }

  test("md5 should produce 32 character hash") {
    val text = "Test content"
    val hash = HashUtils.md5(text)

    assert(hash.length == 32)
  }
}