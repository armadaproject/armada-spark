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

object ArmadaUtilsExceptions {
  class MasterUrlParsingException extends RuntimeException
}

object ArmadaUtils {
  import ArmadaUtilsExceptions._

  def parseMasterUrl(masterUrl: String): (String, Int) = {
    val tokens = masterUrl.substring("armada://".length).split(":")
    if (tokens.length != 2) {
      throw new MasterUrlParsingException
    }
    val host: String = tokens(0)
    val port: Int = try {
      tokens(1).toInt
    } catch {
      case e: NumberFormatException => -1
    }
    if (port < 0) {
      throw new MasterUrlParsingException
    }
    (host, port)
  }
}
