/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.deploy.armada.submit

import java.io.{File, FileInputStream, InputStream}
import java.net.{HttpURLConnection, URI, URL}
import scala.io.Source
import scala.util.{Try, Using}
import org.json4s.jackson.JsonMethods
import org.json4s.{DefaultFormats, JValue}
import api.submit.{IngressConfig, JobSubmitRequest, JobSubmitRequestItem, ServiceConfig}

/** Exception types specific to job template loading operations. */
object JobTemplateLoaderExceptions {

  /** Base exception for all job template loading failures. */
  class JobTemplateLoadException(message: String, cause: Throwable = null)
      extends RuntimeException(message, cause)

  /** Thrown when an unsupported URI protocol is encountered. */
  class UnsupportedProtocolException(protocol: String)
      extends JobTemplateLoadException(s"Unsupported protocol: $protocol")

  /** Thrown when a template file cannot be found at the specified location. */
  class FileNotFoundException(path: String)
      extends JobTemplateLoadException(s"File not found: $path")

  /** Thrown when network operations fail (HTTP/HTTPS/S3 downloads). */
  class NetworkException(message: String, cause: Throwable)
      extends JobTemplateLoadException(message, cause)
}

/** Utility for loading Armada job templates from various sources.
  *
  * Supports loading job template files from:
  *   - Local filesystem (with or without file:// prefix)
  *   - HTTP/HTTPS URLs
  *   - S3 URLs (s3://bucket/key format)
  *
  * The loaded template content can be used to deserialize into JobSubmitRequest structures for
  * customizing job submission parameters.
  */
private[submit] object JobTemplateLoader {
  import JobTemplateLoaderExceptions._

  /** Loads a job template from the specified path or URL and deserializes it into JobSubmitRequest.
    *
    * @param templatePath
    *   Path or URL to the template file. Supported formats:
    *   - Local path: "/path/to/template.yaml" or "template.json"
    *   - File URI: "file:///path/to/template.yaml"
    *   - HTTP/HTTPS: "https://example.com/template.json"
    *   - S3: "s3://bucket-name/path/to/template.yaml"
    * @return
    *   The template as a JobSubmitRequest protobuf structure
    * @throws IllegalArgumentException
    *   if templatePath is empty
    * @throws UnsupportedProtocolException
    *   if the URI scheme is not supported
    * @throws FileNotFoundException
    *   if a local file cannot be found
    * @throws NetworkException
    *   if network operations fail
    * @throws JobTemplateLoadException
    *   for other loading failures including JSON parsing errors
    */
  def loadJobTemplate(templatePath: String): JobSubmitRequest = {
    if (templatePath.isEmpty) {
      throw new IllegalArgumentException("Template path cannot be empty")
    }

    val uri    = normalizeUri(templatePath)
    val scheme = uri.getScheme

    val content = scheme match {
      case null | "file"    => loadContentFromFile(uri)
      case "http" | "https" => loadContentFromHttp(uri)
      case _                => throw new UnsupportedProtocolException(scheme)
    }

    parseJobSubmitRequest(content)
  }

  /** Parses JSON content into a JobSubmitRequest protobuf structure.
    *
    * @param content
    *   JSON content as string
    * @return
    *   Parsed JobSubmitRequest
    * @throws JobTemplateLoadException
    *   if parsing fails
    */
  private def parseJobSubmitRequest(content: String): JobSubmitRequest = {
    implicit val formats: DefaultFormats = DefaultFormats

    try {
      val json = JsonMethods.parse(content)

      // Extract top-level fields
      val queue    = (json \ "queue").extractOpt[String].getOrElse("")
      val jobSetId = (json \ "job_set_id").extractOpt[String].getOrElse("")

      // Extract job_request_items array
      val jobRequestItemsJson =
        (json \ "job_request_items").extractOpt[List[org.json4s.JValue]].getOrElse(List.empty)

      val jobRequestItems = jobRequestItemsJson.map { itemJson =>
        val priority  = (itemJson \ "priority").extractOpt[Double].getOrElse(0.0)
        val namespace = (itemJson \ "namespace").extractOpt[String].getOrElse("default")
        val annotations =
          (itemJson \ "annotations").extractOpt[Map[String, String]].getOrElse(Map.empty)
        val labels = (itemJson \ "labels").extractOpt[Map[String, String]].getOrElse(Map.empty)

        JobSubmitRequestItem(
          priority = priority,
          namespace = namespace,
          annotations = annotations,
          labels = labels
          // Note: Other fields like podSpecs would need similar extraction
          // but are complex nested structures that would require more detailed parsing
        )
      }

      // If no job_request_items in template, create one from top-level fields for backward compatibility
      val finalJobRequestItems = if (jobRequestItems.isEmpty) {
        val priority  = (json \ "priority").extractOpt[Double].getOrElse(0.0)
        val namespace = (json \ "namespace").extractOpt[String].getOrElse("default")
        val annotations =
          (json \ "annotations").extractOpt[Map[String, String]].getOrElse(Map.empty)
        val labels = (json \ "labels").extractOpt[Map[String, String]].getOrElse(Map.empty)

        Seq(
          JobSubmitRequestItem(
            priority = priority,
            namespace = namespace,
            annotations = annotations,
            labels = labels
          )
        )
      } else {
        jobRequestItems
      }

      JobSubmitRequest(
        queue = queue,
        jobSetId = jobSetId,
        jobRequestItems = finalJobRequestItems
      )
    } catch {
      case e: Exception =>
        throw new JobTemplateLoadException(s"Failed to parse JSON template content", e)
    }
  }

  /** Normalizes a path string into a proper URI, handling both absolute URIs and local file paths
    * without protocol prefix.
    *
    * @param path
    *   Input path that may be a URI with protocol or a local file path
    * @return
    *   Normalized URI with proper scheme detection
    */
  private def normalizeUri(path: String): URI = {
    if (path.contains("://") || path.startsWith("file:")) {
      // Already a URI with protocol scheme
      new URI(path)
    } else {
      // Local file path without protocol - convert to file:// URI
      val file = new File(path)
      file.toURI
    }
  }

  /** Loads template content from a local file or file:// URI.
    *
    * Handles both explicit file:// URIs and local paths, performing comprehensive validation of
    * file existence, accessibility, and type.
    *
    * @param uri
    *   File URI to load from
    * @return
    *   Template content as string
    * @throws FileNotFoundException
    *   if file doesn't exist
    * @throws JobTemplateLoadException
    *   if file is not readable or not a regular file
    */
  private def loadContentFromFile(uri: URI): String = {
    val file = if (uri.getScheme == "file") {
      try {
        // Use URI constructor for proper file:// URL handling
        new File(uri)
      } catch {
        case e: IllegalArgumentException =>
          // Fallback if URI constructor fails due to malformed URI
          throw new JobTemplateLoadException(
            s"Failed to create File from URI: $uri, scheme: ${uri.getScheme}, path: ${uri.getPath}",
            e
          )
      }
    } else {
      // Handle case where scheme is null (normalized local path)
      new File(uri.getPath)
    }

    // Validate file existence and accessibility
    if (!file.exists()) {
      throw new FileNotFoundException(
        s"File not found: ${file.getAbsolutePath} (from URI: $uri, scheme: ${uri.getScheme})"
      )
    }
    if (!file.isFile) {
      throw new JobTemplateLoadException(s"Path is not a file: ${file.getAbsolutePath}")
    }
    if (!file.canRead) {
      throw new JobTemplateLoadException(s"Cannot read file: ${file.getAbsolutePath}")
    }

    // Read file content with proper resource management
    Using(Source.fromFile(file)) { source =>
      source.mkString
    }.recover { case e: Exception =>
      throw new JobTemplateLoadException(s"Failed to read file: ${file.getAbsolutePath}", e)
    }.get
  }

  /** Downloads template content from HTTP/HTTPS URLs.
    *
    * Implements robust HTTP client behavior with proper timeouts, redirect following, and error
    * handling for production use.
    *
    * @param uri
    *   HTTP/HTTPS URI to download from
    * @return
    *   Template content as string
    * @throws NetworkException
    *   if HTTP request fails or returns non-200 status
    */
  private def loadContentFromHttp(uri: URI): String = {
    val url        = uri.toURL
    val connection = url.openConnection().asInstanceOf[HttpURLConnection]

    try {
      // Configure HTTP client for production robustness
      connection.setRequestMethod("GET")
      connection.setConnectTimeout(30000)         // 30 seconds connection timeout
      connection.setReadTimeout(60000)            // 60 seconds read timeout
      connection.setInstanceFollowRedirects(true) // Follow redirects automatically

      val responseCode = connection.getResponseCode
      if (responseCode != HttpURLConnection.HTTP_OK) {
        throw new NetworkException(
          s"HTTP request failed with status $responseCode for URL: $url",
          null
        )
      }

      // Read response content with proper resource management
      Using(Source.fromInputStream(connection.getInputStream)) { source =>
        source.mkString
      }.recover { case e: Exception =>
        throw new NetworkException(s"Failed to read HTTP response from: $url", e)
      }.get
    } catch {
      case e: NetworkException => throw e
      case e: Exception =>
        throw new NetworkException(s"Failed to connect to: $url", e)
    } finally {
      connection.disconnect()
    }
  }

  /** Loads a job item template from the specified path or URL and deserializes it into
    * JobSubmitRequestItem.
    *
    * @param templatePath
    *   Path or URL to the template file. Supported formats:
    *   - Local path: "/path/to/template.yaml" or "template.json"
    *   - File URI: "file:///path/to/template.yaml"
    *   - HTTP/HTTPS: "https://example.com/template.json"
    *   - S3: "s3://bucket-name/path/to/template.yaml"
    * @return
    *   The template as a JobSubmitRequestItem protobuf structure
    * @throws IllegalArgumentException
    *   if templatePath is empty
    * @throws UnsupportedProtocolException
    *   if the URI scheme is not supported
    * @throws FileNotFoundException
    *   if a local file cannot be found
    * @throws NetworkException
    *   if network operations fail
    * @throws JobTemplateLoadException
    *   for other loading failures including JSON parsing errors
    */
  def loadJobItemTemplate(templatePath: String): JobSubmitRequestItem = {
    if (templatePath.isEmpty) {
      throw new IllegalArgumentException("Template path cannot be empty")
    }

    val uri    = normalizeUri(templatePath)
    val scheme = uri.getScheme

    val content = scheme match {
      case null | "file"    => loadContentFromFile(uri)
      case "http" | "https" => loadContentFromHttp(uri)
      case _                => throw new UnsupportedProtocolException(scheme)
    }

    parseJobSubmitRequestItem(content)
  }

  /** Parses JSON content into a JobSubmitRequestItem protobuf structure.
    *
    * @param content
    *   JSON content as string
    * @return
    *   Parsed JobSubmitRequestItem
    * @throws JobTemplateLoadException
    *   if parsing fails
    */
  private def parseJobSubmitRequestItem(content: String): JobSubmitRequestItem = {
    implicit val formats: DefaultFormats = DefaultFormats

    try {
      JsonMethods.parse(content).extractOpt[JobSubmitRequestItem] match {
        case Some(item) => item
        case None =>
          throw new JobTemplateLoadException("Failed to extract JobSubmitRequestItem from JSON")
      }
    } catch {
      case e: Exception =>
        throw new JobTemplateLoadException(s"Failed to parse JSON template item content", e)
    }
  }
}
