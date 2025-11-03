package com.cs441.hw2

import java.security.MessageDigest

/**
 * Utilities for computing cryptographic hashes.
 *
 * Design rationale:
 * - Deterministic IDs for idempotent operations
 * - SHA-256 for content hashing (change detection)
 * - MD5 for lightweight checksums
 */
object HashUtils {

  /**
   * Compute SHA-256 hash of string content.
   */
  def sha256(content: String): String = {
    val digest = MessageDigest.getInstance("SHA-256")
    val hashBytes = digest.digest(content.getBytes("UTF-8"))
    hashBytes.map("%02x".format(_)).mkString
  }

  /**
   * Compute MD5 hash of string content.
   */
  def md5(content: String): String = {
    val digest = MessageDigest.getInstance("MD5")
    val hashBytes = digest.digest(content.getBytes("UTF-8"))
    hashBytes.map("%02x".format(_)).mkString
  }

  /**
   * Generate deterministic chunk ID from document, position, and content.
   */
  def generateChunkId(documentId: String, chunkIndex: Int, chunkContent: String): String = {
    val chunkHash = sha256(chunkContent)
    val hashPrefix = chunkHash.take(8)
    s"${documentId}_${chunkIndex}_$hashPrefix"
  }

  /**
   * Generate embedding ID from chunk and model version.
   */
  def generateEmbeddingId(chunkId: String, modelVersion: String): String = {
    s"${chunkId}_${modelVersion.replaceAll("[^a-zA-Z0-9]", "_")}"
  }

  /**
   * Generate document ID from file path.
   */
  def generateDocumentId(filePath: String): String = {
    val normalizedPath = filePath.replaceAll("\\\\", "/")
    val hash = sha256(normalizedPath)
    s"doc_${hash.take(16)}"
  }
}