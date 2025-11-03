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
 * Client for interacting with Ollama API using basic HTTP (no Circe/Cats dependencies)
 * Design rationale: Uses functional Iterator pattern to avoid mutable variables and while loops
 */
class OllamaClient(baseUrl: String) extends LazyLogging {
  implicit val formats: DefaultFormats.type = DefaultFormats

  private val embedUrl = s"$baseUrl/api/embeddings"
  private val connectionTimeout = 30000 // 30 seconds
  private val readTimeout = 60000 // 60 seconds

  /**
   * Generate embedding for a text using Ollama API
   */
  def generateEmbedding(text: String, model: String = "nomic-embed-text"): Array[Double] = {
    try {
      // Create request body
      val requestBody = compact(render(
        ("model" -> model) ~
          ("prompt" -> text)
      ))

      // Make HTTP POST request
      val response = makeHttpPost(embedUrl, requestBody)

      // Parse response
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
   * Make HTTP POST request using basic Java HTTP
   * Refactored to use functional Iterator pattern instead of while loop
   */
  private def makeHttpPost(url: String, body: String): String = {
    // Use Option to handle null connection safely
    var connection: HttpURLConnection = null // NOTE: Java interop requires var for connection management
    try {
      val urlObj = new URL(url)
      connection = urlObj.openConnection().asInstanceOf[HttpURLConnection]

      // Configure connection
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

      // Read response
      val responseCode = connection.getResponseCode
      if (responseCode != 200) {
        throw new RuntimeException(s"HTTP error code: $responseCode")
      }

      val inputStream = connection.getInputStream
      try {
        // Functional approach: use Source.fromInputStream which returns Iterator
        // No var, no while loop - functional line reading
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
   * Test connection to Ollama API
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