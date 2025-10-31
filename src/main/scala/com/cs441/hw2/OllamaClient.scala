package com.cs441.hw2

import com.typesafe.scalalogging.LazyLogging
import org.json4s._
import org.json4s.native.JsonMethods._
import org.json4s.JsonDSL._

import java.io.{BufferedReader, InputStreamReader, OutputStream}
import java.net.{HttpURLConnection, URL}
import java.nio.charset.StandardCharsets

/**
 * Client for interacting with Ollama API using basic HTTP (no Circe/Cats dependencies)
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
   */
  private def makeHttpPost(url: String, body: String): String = {
    var connection: HttpURLConnection = null
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
      val reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))
      try {
        val response = new StringBuilder()
        var line = reader.readLine()
        while (line != null) {
          response.append(line)
          line = reader.readLine()
        }
        response.toString
      } finally {
        reader.close()
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