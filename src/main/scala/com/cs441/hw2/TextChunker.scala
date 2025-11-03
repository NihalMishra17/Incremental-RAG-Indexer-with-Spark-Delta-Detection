package com.cs441.hw2

import com.typesafe.scalalogging.LazyLogging

/**
 * Text chunking utility with overlap support.
 *
 * Design rationale:
 * - Functional approach using Range and map (no mutable state)
 * - Fixed-size chunks with configurable overlap for context preservation
 * - Deterministic chunking ensures same text always produces same chunks
 */
object TextChunker extends LazyLogging {

  /**
   * Chunk text into overlapping segments using functional approach.
   * No var, no while loops - uses Range for functional iteration.
   */
  def chunk(text: String, chunkSize: Int, overlap: Int): Seq[String] = {
    // Input validation
    if (chunkSize <= 0) {
      logger.error(s"Invalid chunk size: $chunkSize")
      return Seq.empty
    }

    if (overlap < 0 || overlap >= chunkSize) {
      logger.error(s"Invalid overlap: $overlap (must be 0 <= overlap < chunkSize)")
      return Seq.empty
    }

    if (text.trim.isEmpty) {
      logger.warn("Attempting to chunk empty text")
      return Seq.empty
    }

    val cleanedText = text.trim

    // If text fits in one chunk, return as-is
    if (cleanedText.length <= chunkSize) {
      return Seq(cleanedText)
    }

    // Functional approach: use Range to generate start positions
    val step = chunkSize - overlap

    // Generate all chunk start positions
    val startPositions = (0 until cleanedText.length by step).toSeq

    // Map each start position to a substring (no mutable state)
    startPositions.map { start =>
      val end = math.min(start + chunkSize, cleanedText.length)
      cleanedText.substring(start, end)
    }.filter(_.nonEmpty)

  }

  /**
   * Chunk text and return with indices.
   * Uses functional zipWithIndex.
   */
  def chunkWithIndex(text: String, chunkSize: Int, overlap: Int): Seq[(Int, String)] = {
    chunk(text, chunkSize, overlap).zipWithIndex.map { case (chunk, idx) =>
      (idx, chunk)  // Return (index, chunk) not (chunk, index)
    }
  }

  /**
   * Alternative implementation using Stream for lazy evaluation.
   */
  def chunkUnfold(text: String, chunkSize: Int, overlap: Int): Seq[String] = {
    if (chunkSize <= 0 || overlap < 0 || overlap >= chunkSize) {
      return Seq.empty
    }

    val cleanedText = text.trim
    if (cleanedText.isEmpty || cleanedText.length <= chunkSize) {
      return if (cleanedText.isEmpty) Seq.empty else Seq(cleanedText)
    }

    val step = chunkSize - overlap

    // Stream for lazy chunk generation
    Stream.iterate(0)(_ + step)
      .takeWhile(_ < cleanedText.length)
      .map { position =>
        val end = math.min(position + chunkSize, cleanedText.length)
        cleanedText.substring(position, end)
      }
      .toSeq
  }
}