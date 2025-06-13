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

import scala.util.Try

object ArmadaUtilsExceptions {
  class MasterUrlParsingException extends RuntimeException
}

object ArmadaUtils {
  import ArmadaUtilsExceptions._

  def parseMasterUrl(masterUrl: String): (String, Int) = {
    Some(masterUrl)
      .map(_.substring("armada://".length).split(":").toSeq)
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
        """.stripMargin.trim

  def getExecutorRange(numberOfExecutors: Int): Range = {
    0 until numberOfExecutors
  }
}
