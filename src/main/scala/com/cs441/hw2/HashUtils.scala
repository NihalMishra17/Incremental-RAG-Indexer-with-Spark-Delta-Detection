package com.cs441.hw2

import java.security.MessageDigest

/**
 * Utilities for computing cryptographic hashes of content.
 */
object HashUtils {

  def sha256(content: String): String = {
    val digest = MessageDigest.getInstance("SHA-256")
    val hashBytes = digest.digest(content.getBytes("UTF-8"))
    hashBytes.map("%02x".format(_)).mkString
  }

  def md5(content: String): String = {
    val digest = MessageDigest.getInstance("MD5")
    val hashBytes = digest.digest(content.getBytes("UTF-8"))
    hashBytes.map("%02x".format(_)).mkString
  }

  def generateChunkId(documentId: String, chunkIndex: Int, chunkContent: String): String = {
    val chunkHash = sha256(chunkContent)
    val hashPrefix = chunkHash.take(8)
    s"${documentId}_${chunkIndex}_$hashPrefix"
  }

  def generateEmbeddingId(chunkId: String, modelVersion: String): String = {
    s"${chunkId}_${modelVersion.replaceAll("[^a-zA-Z0-9]", "_")}"
  }

  def generateDocumentId(filePath: String): String = {
    val normalizedPath = filePath.replaceAll("\\\\", "/")
    val hash = sha256(normalizedPath)
    s"doc_${hash.take(16)}"
  }
}