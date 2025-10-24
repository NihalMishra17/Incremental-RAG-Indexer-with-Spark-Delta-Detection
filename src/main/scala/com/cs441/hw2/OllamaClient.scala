package com.cs441.hw2

import com.typesafe.scalalogging.LazyLogging
import io.circe._
import io.circe.parser._
import io.circe.generic.auto._
import sttp.client3._
import sttp.client3.circe._

import scala.concurrent.duration._

/**
 * Client for interacting with Ollama API to generate embeddings.
 */
class OllamaClient(host: String, model: String) extends LazyLogging {

  private val backend = HttpURLConnectionBackend()
  private val embedUrl = s"$host/api/embeddings"

  case class EmbedRequest(model: String, prompt: String)
  case class EmbedResponse(embedding: List[Double])

  def generateEmbedding(text: String): Either[String, Array[Float]] = {
    try {
      val request = EmbedRequest(model, text)

      val response = basicRequest
        .post(uri"$embedUrl")
        .body(request)
        .response(asJson[EmbedResponse])
        .readTimeout(60.seconds)
        .send(backend)

      response.body match {
        case Right(embedResponse) =>
          val embedding = embedResponse.embedding.map(_.toFloat).toArray
          logger.debug(s"Generated embedding of dimension ${embedding.length}")
          Right(embedding)
        case Left(error) =>
          val errorMsg = s"Failed to generate embedding: $error"
          logger.error(errorMsg)
          Left(errorMsg)
      }
    } catch {
      case e: Exception =>
        val errorMsg = s"Exception generating embedding: ${e.getMessage}"
        logger.error(errorMsg, e)
        Left(errorMsg)
    }
  }

  def generateEmbeddingsBatch(texts: List[String]): List[(String, Array[Float])] = {
    logger.info(s"Generating embeddings for batch of ${texts.size} texts")

    val results = texts.map { text =>
      generateEmbedding(text) match {
        case Right(embedding) => Some((text, embedding))
        case Left(error) =>
          logger.warn(s"Failed to generate embedding for text: ${text.take(50)}... - $error")
          None
      }
    }

    val successful = results.flatten
    logger.info(s"Successfully generated ${successful.size}/${texts.size} embeddings")
    successful
  }

  def close(): Unit = {
    backend.close()
  }
}

object OllamaClient {
  def fromConfig(config: Configuration.EmbeddingConfig): OllamaClient = {
    new OllamaClient(config.ollamaHost, config.model)
  }
}
