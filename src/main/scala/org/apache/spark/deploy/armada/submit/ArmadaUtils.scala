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

import org.apache.spark.SparkConf
import org.apache.spark.deploy.armada.Config
import scala.util.Try
import scala.sys.process._

object ArmadaUtilsExceptions {
  class MasterUrlParsingException extends RuntimeException
}
object ArmadaUtils {
  import ArmadaUtilsExceptions._

  def parseMasterUrl(masterUrl: String): (String, Int) = {
    val startString = if (masterUrl.startsWith("local")) {
      "local://armada://"
    } else {
      "armada://"
    }
    Some(masterUrl)
      .map(_.substring(startString.length).split(":").toSeq)
      .filter(_.length == 2)
      .map { case Seq(host: String, portString: String) =>
        (host, Try(portString.toInt).getOrElse(-1))
      }
      .filter(_._2 >= 0)
      .getOrElse(throw new MasterUrlParsingException)
  }

  def buildServiceNameFromJobId(jobId: String): String = s"armada-$jobId-0-service-0"

  val initContainerCommand: String =
    """
          echo "starting to connect to driver $SPARK_DRIVER_HOST"
          start_time=$(date +%s);
          timeout=$SPARK_EXECUTOR_CONNECTION_TIMEOUT;
          while ! nc -z $SPARK_DRIVER_HOST $SPARK_DRIVER_PORT; do
            now=$(date +%s);
            elapsed=$((now - start_time));
            if [ $elapsed -ge $timeout ]; then
              echo "Timeout waiting for driver after ${timeout}s";
              exit 1;
            fi;
            echo "waiting for driver...";
            sleep 1;
          done
          echo "driver $SPARK_DRIVER_HOST found"
        """.stripMargin.trim

  def getExecutorRange(numberOfExecutors: Int): Range = {
    0 until numberOfExecutors
  }

  /** Sets a default application ID in SparkConf if not already set.
    *
    * If spark.app.id is not already set, generates a default application ID with the format
    * "armada-spark-app-id-<UUID>".
    *
    * @param conf
    *   Spark configuration to check and potentially update
    */
  def setDefaultAppId(conf: SparkConf): Unit = {
    if (conf.getOption("spark.app.id").isEmpty) {
      val defaultAppId =
        s"armada-spark-app-id-${java.util.UUID.randomUUID().toString.replaceAll("-", "")}"
      conf.set("spark.app.id", defaultAppId)
    }
  }

  /** Gets the application ID from SparkConf, generating a default if not set.
    *
    * @param conf
    *   Spark configuration
    * @return
    *   The application ID string
    */
  def getApplicationId(conf: SparkConf): String = {
    setDefaultAppId(conf)
    conf.get("spark.app.id")
  }

  /** Get auth token from authentication script.
    *
    * Executes the script specified in spark.armada.auth.script.path and returns its output as the token.
    * Returns None if the script path is not configured or if the script execution fails.
    *
    * @param conf
    *   Optional Spark configuration to read script path from
    * @return
    *   Some(token) if found, None otherwise
    */
  def getAuthToken(conf: Option[SparkConf] = None): Option[String] = {
    conf.flatMap(_.get(Config.ARMADA_AUTH_SCRIPT_PATH)) match {
      case Some(scriptPath) =>
        val authScript = new java.io.File(scriptPath)
        if (authScript.exists() && authScript.canExecute) {
          try {
            val token = Seq("sh", authScript.getAbsolutePath).!!.trim
            if (token.nonEmpty) Some(token) else None
          } catch {
            case e: Exception =>
              None
          }
        } else {
          None
        }
      case None =>
        None
    }
  }
}
