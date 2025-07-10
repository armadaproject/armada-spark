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

import api.submit.{JobSubmitRequest, JobSubmitRequestItem}
import com.fasterxml.jackson.core.{JsonParser, JsonProcessingException}
import com.fasterxml.jackson.databind.{DeserializationContext, DeserializationFeature, JsonDeserializer, ObjectMapper}
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import k8s.io.apimachinery.pkg.api.resource.generated.Quantity

import java.io.File
import java.net.{HttpURLConnection, URL}
import scala.io.Source
import scala.util.{Failure, Success, Try, Using}

/** Utility class for loading job templates from various sources.
  *
  * Uses Jackson for parsing YAML content directly to Armada job objects. Automatically discards
  * unknown fields (like apiVersion, kind) from Kubernetes YAML files.
  *
  * Supports loading templates from:
  *   - Local files (with or without file:// prefix)
  *   - HTTP/HTTPS URLs
  *   - YAML format
  */
private[spark] object JobTemplateLoader {

  private val yamlMapper: ObjectMapper = {
    val mapper = new ObjectMapper(new YAMLFactory())
    mapper.registerModule(DefaultScalaModule)
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

    // Register custom deserializer for Quantity class
    val module = new SimpleModule()
    module.addDeserializer(classOf[Quantity], new QuantityDeserializer())
    mapper.registerModule(module)

    mapper
  }

  /**
   * Custom deserializer for Quantity objects.
   * Handles converting string values like '1433Mi' into Quantity objects.
   */
  private class QuantityDeserializer extends JsonDeserializer[Quantity] {
    @throws(classOf[JsonProcessingException])
    override def deserialize(p: JsonParser, ctxt: DeserializationContext): Quantity = {
      val value = p.getValueAsString
      if (value == null || value.isEmpty) {
        null
      } else {
        new Quantity(Option(value))
      }
    }
  }

  /** Loads a JobSubmitRequest template from the specified path.
    *
    * @param templatePath
    *   Path to the template file (local file or HTTP/HTTPS URL)
    * @return
    *   Loaded JobSubmitRequest template
    * @throws RuntimeException
    *   if template cannot be loaded or parsed
    */
  def loadJobTemplate(templatePath: String): JobSubmitRequest = {
    val content = loadTemplateContent(templatePath)
    unmarshal(content, classOf[JobSubmitRequest], templatePath)
  }

  /** Loads a JobSubmitRequestItem template from the specified path.
    *
    * @param templatePath
    *   Path to the template file (local file or HTTP/HTTPS URL)
    * @return
    *   Loaded JobSubmitRequestItem template
    * @throws RuntimeException
    *   if template cannot be loaded or parsed
    */
  def loadJobItemTemplate(templatePath: String): JobSubmitRequestItem = {
    val content = loadTemplateContent(templatePath)
    unmarshal(content, classOf[JobSubmitRequestItem], templatePath)
  }

  /** Loads template content from various sources.
    *
    * @param templatePath
    *   Path to the template (file, HTTP, HTTPS)
    * @return
    *   Template content as string
    */
  private def loadTemplateContent(templatePath: String): String = {
    val lowerPath = templatePath.toLowerCase

    if (lowerPath.startsWith("http://") || lowerPath.startsWith("https://")) {
      loadFromUrl(templatePath)
    } else if (lowerPath.startsWith("file://")) {
      loadFromFile(templatePath.substring(7)) // Remove "file://" prefix
    } else {
      loadFromFile(templatePath) // Local file without protocol
    }
  }

  // Loads template content from a URL (HTTP/HTTPS).
  private def loadFromUrl(url: String): String = {
    Try {
      val connection = new URL(url).openConnection().asInstanceOf[HttpURLConnection]
      connection.setRequestMethod("GET")
      connection.setConnectTimeout(10000) // 10 seconds
      connection.setReadTimeout(30000)    // 30 seconds

      Using(connection.getInputStream) { inputStream =>
        Source.fromInputStream(inputStream, "UTF-8").mkString
      }.get
    } match {
      case Success(content) => content
      case Failure(exception) =>
        throw new RuntimeException(s"Failed to load template from URL: $url", exception)
    }
  }

  // Loads template content from a local file.
  private def loadFromFile(filePath: String): String = {
    Try {
      val file = new File(filePath)
      if (!file.exists()) {
        throw new RuntimeException(s"Template file does not exist: $filePath")
      }
      if (!file.isFile) {
        throw new RuntimeException(s"Template path is not a file: $filePath")
      }
      if (!file.canRead) {
        throw new RuntimeException(s"Cannot read template file: $filePath")
      }

      Using(Source.fromFile(file, "UTF-8")) { source =>
        source.mkString
      }.get
    } match {
      case Success(content) => content
      case Failure(exception) =>
        throw new RuntimeException(s"Failed to load template from file: $filePath", exception)
    }
  }

  /** Unmarshals YAML content directly to the target type using Jackson.
    *
    * @param content
    *   Template content string (YAML format)
    * @param clazz
    *   Target class for deserialization
    * @param templatePath
    *   Original template path (for error messages)
    * @tparam T
    *   Type of the template object
    * @return
    *   Parsed template object
    */
  def unmarshal[T](content: String, clazz: Class[T], templatePath: String): T = {
    Try {
      yamlMapper.readValue(content, clazz)
    } match {
      case Success(template) => template
      case Failure(exception) =>
        throw new RuntimeException(
          s"Failed to parse template as YAML from: $templatePath. " +
            s"Error: ${exception.getMessage}",
          exception
        )
    }
  }
}
