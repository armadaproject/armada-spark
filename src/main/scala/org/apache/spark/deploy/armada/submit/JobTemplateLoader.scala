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
import com.fasterxml.jackson.databind.{
  DeserializationContext,
  DeserializationFeature,
  JsonDeserializer,
  JsonNode,
  ObjectMapper
}
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import k8s.io.api.core.v1.generated.PodSpec
import io.fabric8.kubernetes.api.model
import io.fabric8.kubernetes.client.utils.Serialization

import java.io.File
import java.net.{HttpURLConnection, URL}
import scala.io.Source
import scala.util.{Failure, Success, Try}

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

    // The generated Scala classes from the PodSpec proto differ from standard Kubernetes PodSpec manifest and fabric8 PodSpec model.
    // We use a custom deserializer which uses fabric8's Serialization to parse YAML to fabric8 PodSpec,
    // then converts to protobuf using PodSpecConverter.
    val module = new SimpleModule()
    module.addDeserializer(classOf[PodSpec], new PodSpecDeserializer())
    mapper.registerModule(module)

    mapper
  }

  /** Custom deserializer for PodSpec objects.
    *
    * Uses fabric8's Serialization.unmarshal() to parse YAML to fabric8 PodSpec, then converts to
    * protobuf using PodSpecConverter.
    */
  private class PodSpecDeserializer extends JsonDeserializer[PodSpec] {
    @throws(classOf[JsonProcessingException])
    override def deserialize(p: JsonParser, ctxt: DeserializationContext): PodSpec = {
      val node = p.getCodec.readTree(p).asInstanceOf[JsonNode]

      // Convert JsonNode to YAML string, then use fabric8's Serialization to parse it
      val mapper     = new ObjectMapper(new YAMLFactory())
      val yamlString = mapper.writeValueAsString(node)

      // Use fabric8's Serialization.unmarshal() which handles all Kubernetes types (including Quantity)
      val fabric8PodSpec = Serialization.unmarshal(yamlString, classOf[model.PodSpec])

      PodSpecConverter.fabric8ToProtobuf(fabric8PodSpec)
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

      val inputStream = connection.getInputStream
      try {
        Source.fromInputStream(inputStream, "UTF-8").mkString
      } finally {
        inputStream.close()
      }
    } match {
      case Success(content) => content
      case Failure(exception) =>
        throw new RuntimeException(
          s"Failed to load template from URL: $url (${exception.getClass.getSimpleName}: ${exception.getMessage})",
          exception
        )
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

      val source = Source.fromFile(file, "UTF-8")
      try {
        source.mkString
      } finally {
        source.close()
      }
    } match {
      case Success(content) => content
      case Failure(exception) =>
        throw new RuntimeException(
          s"Failed to load template from file: $filePath (${exception.getClass.getSimpleName}: ${exception.getMessage})",
          exception
        )
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
  private[submit] def unmarshal[T](content: String, clazz: Class[T], templatePath: String): T = {
    Try {
      yamlMapper.readValue(content, clazz)
    } match {
      case Success(template) => template
      case Failure(exception) =>
        throw new RuntimeException(
          s"Failed to parse template as YAML from: $templatePath " +
            s"(${exception.getClass.getSimpleName}: ${exception.getMessage})",
          exception
        )
    }
  }
}
