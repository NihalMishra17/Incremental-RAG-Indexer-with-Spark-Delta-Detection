package com.cs441.hw2

import com.typesafe.scalalogging.LazyLogging
import org.json4s._
import org.json4s.native.JsonMethods._
import org.json4s.JsonDSL._

import java.io.{BufferedReader, InputStreamReader, OutputStream}
import java.net.{HttpURLConnection, URL}
import java.nio.charset.StandardCharsets
import scala.io.Source

/**
 * HTTP client for Ollama embedding API.
 *
 * Design rationale:
 * - Uses basic Java HTTP to avoid heavy dependencies (no Circe/Cats)
 * - Functional Iterator pattern for response reading (no while loops)
 * - Lightweight json4s for JSON parsing
 * - Connection pooling not needed for homework scale
 */
class OllamaClient(baseUrl: String) extends LazyLogging {
  implicit val formats: DefaultFormats.type = DefaultFormats

  private val embedUrl = s"$baseUrl/api/embeddings"
  private val connectionTimeout = 30000 // 30 seconds
  private val readTimeout = 60000 // 60 seconds for large text

  /**
   * Generate embedding vector for text using specified model.
   * Returns Array[Double] of embedding dimensions (typically 1024).
   */
  def generateEmbedding(text: String, model: String = "nomic-embed-text"): Array[Double] = {
    try {
      // Build JSON request body
      val requestBody = compact(render(
        ("model" -> model) ~
          ("prompt" -> text)
      ))

      // POST to Ollama API
      val response = makeHttpPost(embedUrl, requestBody)

      // Extract embedding array from JSON response
      val json = parse(response)
      val embedding = (json \ "embedding").extract[List[Double]]

      logger.debug(s"Generated embedding of size ${embedding.length}")
      embedding.toArray

    } catch {
      case e: Exception =>
        logger.error(s"Failed to generate embedding: ${e.getMessage}", e)
        throw new RuntimeException(s"Ollama API error: ${e.getMessage}", e)
    }
  }

  /**
   * Make HTTP POST request using Java HttpURLConnection.
   * Uses functional Stream approach for reading response.
   */
  private def makeHttpPost(url: String, body: String): String = {
    var connection: HttpURLConnection = null // Java interop requires var for resource management
    try {
      val urlObj = new URL(url)
      connection = urlObj.openConnection().asInstanceOf[HttpURLConnection]

      // Configure connection parameters
      connection.setRequestMethod("POST")
      connection.setDoOutput(true)
      connection.setConnectTimeout(connectionTimeout)
      connection.setReadTimeout(readTimeout)
      connection.setRequestProperty("Content-Type", "application/json; charset=UTF-8")
      connection.setRequestProperty("Accept", "application/json")

      // Write request body
      val outputStream: OutputStream = connection.getOutputStream
      try {
        val input = body.getBytes(StandardCharsets.UTF_8)
        outputStream.write(input, 0, input.length)
        outputStream.flush()
      } finally {
        if (outputStream != null) outputStream.close()
      }

      // Check response code
      val responseCode = connection.getResponseCode
      if (responseCode != 200) {
        throw new RuntimeException(s"HTTP error code: $responseCode")
      }

      val inputStream = connection.getInputStream
      try {
        // Functional line reading using Source.fromInputStream (returns Iterator)
        Source.fromInputStream(inputStream, StandardCharsets.UTF_8.name())
          .getLines()
          .mkString("\n")
      } finally {
        inputStream.close()
      }

    } catch {
      case e: Exception =>
        logger.error(s"HTTP request failed: ${e.getMessage}", e)
        throw e
    } finally {
      if (connection != null) {
        connection.disconnect()
      }
    }
  }

  /**
   * Test connectivity to Ollama API.
   */
  def testConnection(): Boolean = {
    try {
      generateEmbedding("test", "nomic-embed-text")
      logger.info("Successfully connected to Ollama API")
      true
    } catch {
      case e: Exception =>
        logger.error(s"Failed to connect to Ollama API: ${e.getMessage}", e)
        false
    }
  }
}

object OllamaClient {
  def apply(baseUrl: String): OllamaClient = new OllamaClient(baseUrl)
}