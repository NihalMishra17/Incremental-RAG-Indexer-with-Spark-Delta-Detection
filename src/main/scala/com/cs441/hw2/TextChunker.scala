package com.cs441.hw2

import com.typesafe.scalalogging.LazyLogging

/**
 * Handles text chunking using a sliding window approach with overlap.
 */
object TextChunker extends LazyLogging {

  def chunk(text: String, chunkSize: Int, overlap: Int): List[String] = {
    if (text.isEmpty) {
      logger.warn("Attempting to chunk empty text")
      return List.empty
    }

    if (chunkSize <= 0) {
      logger.error(s"Invalid chunk size: $chunkSize")
      return List.empty
    }

    if (overlap < 0 || overlap >= chunkSize) {
      logger.error(s"Invalid overlap: $overlap (must be 0 <= overlap < chunkSize)")
      return List.empty
    }

    val cleanedText = text.trim
    val stride = chunkSize - overlap
    val chunks = List.newBuilder[String]

    var i = 0
    while (i < cleanedText.length) {
      val end = Math.min(i + chunkSize, cleanedText.length)
      val chunk = cleanedText.substring(i, end).trim

      if (chunk.nonEmpty) {
        chunks += chunk
      }

      i += stride

      if (i < cleanedText.length && i + stride >= cleanedText.length && end < cleanedText.length) {
        val tailChunk = cleanedText.substring(i).trim
        if (tailChunk.nonEmpty && tailChunk != chunk) {
          chunks += tailChunk
        }
        i = cleanedText.length
      }
    }

    val result = chunks.result()
    logger.debug(s"Chunked text of length ${cleanedText.length} into ${result.size} chunks")
    result
  }

  def chunkWithIndex(text: String, chunkSize: Int, overlap: Int): List[(Int, String)] = {
    chunk(text, chunkSize, overlap).zipWithIndex.map { case (text, idx) => (idx, text) }
  }
}