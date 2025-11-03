package com.cs441.hw2

import com.typesafe.scalalogging.LazyLogging

/**
 * Text chunking utility with overlap support
 * Design rationale: Uses functional sliding window approach to avoid mutable induction variables
 */
object TextChunker extends LazyLogging {

  /**
   * Chunk text into overlapping segments using functional approach
   * No var, no while loops - uses Range and sliding for functional iteration
   */
  def chunk(text: String, chunkSize: Int, overlap: Int): Seq[String] = {
    // Validation
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

    // If text is smaller than chunk size, return as single chunk
    if (cleanedText.length <= chunkSize) {
      return Seq(cleanedText)
    }

    // Functional approach: use Range with step to generate chunk start positions
    // Then use sliding window to extract chunks
    // No induction variable, no while loop
    val step = chunkSize - overlap

    // Generate all start positions for chunks
    val startPositions = (0 until cleanedText.length by step).toSeq

    // Map each start position to a chunk
    // This is a functional map operation, no mutable state
    startPositions.map { start =>
      val end = math.min(start + chunkSize, cleanedText.length)
      cleanedText.substring(start, end)
    }.filter(_.nonEmpty) // Remove any empty chunks at the end

  }

  /**
   * Chunk text and return with indices
   * Functional approach using zipWithIndex
   */
  def chunkWithIndex(text: String, chunkSize: Int, overlap: Int): Seq[(String, Int)] = {
    // Uses functional zipWithIndex instead of manual counter
    chunk(text, chunkSize, overlap).zipWithIndex.map { case (chunk, idx) =>
      (chunk, idx)
    }
  }

  /**
   * Alternative implementation using unfold for demonstration
   * Pure functional recursive-style chunking
   */
  def chunkUnfold(text: String, chunkSize: Int, overlap: Int): Seq[String] = {
    // Validation
    if (chunkSize <= 0 || overlap < 0 || overlap >= chunkSize) {
      return Seq.empty
    }

    val cleanedText = text.trim
    if (cleanedText.isEmpty || cleanedText.length <= chunkSize) {
      return if (cleanedText.isEmpty) Seq.empty else Seq(cleanedText)
    }

    val step = chunkSize - overlap

    // Use unfold to generate sequence without any mutable state
    // Unfold is a pure functional approach to sequence generation
    Stream.iterate(0)(_ + step)
      .takeWhile(_ < cleanedText.length)
      .map { position =>
        val end = math.min(position + chunkSize, cleanedText.length)
        cleanedText.substring(position, end)
      }
      .toSeq
  }
}