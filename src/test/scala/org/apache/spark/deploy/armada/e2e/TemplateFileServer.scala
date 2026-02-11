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

package org.apache.spark.deploy.armada.e2e

import java.io.File
import java.net.InetSocketAddress
import java.nio.file.Files

import com.sun.net.httpserver.{HttpExchange, HttpServer}

/** Simple HTTP file server for serving template files to Docker containers and KIND pods.
  *
  * @param baseDir
  *   directory containing the files to serve
  */
class TemplateFileServer(baseDir: File) {

  private val defaultHost: String =
    if (sys.props.getOrElse("os.name", "").toLowerCase.contains("mac")) "host.docker.internal"
    else "172.18.0.1"

  private val host: String =
    sys.env.getOrElse("TEMPLATE_SERVER_HOST", defaultHost)

  private val server: HttpServer =
    HttpServer.create(new InetSocketAddress("0.0.0.0", 0), 0)

  server.createContext(
    "/",
    (exchange: HttpExchange) => {
      val filename = exchange.getRequestURI.getPath.stripPrefix("/")
      val file     = new File(baseDir, filename)

      if (
        file.exists() && file.isFile && file.getCanonicalPath.startsWith(baseDir.getCanonicalPath)
      ) {
        val bytes = Files.readAllBytes(file.toPath)
        exchange.getResponseHeaders.set("Content-Type", "application/x-yaml")
        exchange.sendResponseHeaders(200, bytes.length)
        val os = exchange.getResponseBody
        try os.write(bytes)
        finally os.close()
      } else {
        val msg = s"Not found: $filename".getBytes("UTF-8")
        exchange.sendResponseHeaders(404, msg.length)
        val os = exchange.getResponseBody
        try os.write(msg)
        finally os.close()
      }
    }
  )

  def start(): Int = {
    server.start()
    server.getAddress.getPort
  }

  def stop(): Unit = server.stop(0)

  def port: Int = server.getAddress.getPort

  def url(filename: String): String =
    s"http://$host:${server.getAddress.getPort}/$filename"
}
