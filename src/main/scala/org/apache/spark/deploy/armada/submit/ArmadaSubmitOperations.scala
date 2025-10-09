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
import org.apache.spark.deploy.SparkSubmitOperation
import org.apache.spark.util.CommandLineLoggingUtils

private sealed trait ArmadaSubmitOperation extends CommandLineLoggingUtils {}

private[spark] class ArmadaSparkSubmitOperation
    extends SparkSubmitOperation
    with CommandLineLoggingUtils {

  private def isGlob(name: String): Boolean = {
    name.last == '*'
  }

  def execute(
      submissionId: String,
      sparkConf: SparkConf,
      op: ArmadaSubmitOperation
  ): Unit = {
    printMessage("Not yet implemented for Armada.")
    ()
  }

  override def kill(submissionId: String, conf: SparkConf): Unit = {
    printMessage(
      s"Kill not yet implemented in Armada."
    )
  }

  override def printSubmissionStatus(
      submissionId: String,
      conf: SparkConf
  ): Unit = {
    printMessage(
      s"Request Status not yet implemented in Armada."
    )
  }

  override def supports(master: String): Boolean = {
    master.startsWith("armada://")
  }
}
