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

import k8s.io.api.core.v1.generated._
import org.apache.spark.SparkConf
import org.apache.spark.deploy.armada.submit.ConfigGenerator.{ENV_SPARK_CONF_DIR, REMOTE_CONF_DIR_NAME}

import java.io.File
import scala.io.Source
import scala.util.Using

private[submit] object ConfigGenerator {
  val REMOTE_CONF_DIR_NAME = "/opt/spark/conf"
  val ENV_SPARK_CONF_DIR = "SPARK_CONF_DIR"
}

private[submit] class ConfigGenerator(val prefix: String, val conf: SparkConf) {
  private val confDir = Option(conf.getenv(ENV_SPARK_CONF_DIR)).orElse(
    conf.getOption("spark.home").map(dir => s"$dir/conf"))

  private val confFiles = getConfFiles
  private def getConfFiles : Array[File] = {
    confDir.map(new File(_))
      .filter(_.isDirectory)
      .map(_.listFiles.filter(!_.isDirectory))
      .getOrElse(Array.empty)
  }


  // Store the config files as annotations in the armada submit request
  def getAnnotations: Map[String, String] = {
    confFiles
      .map(f => prefix + "/" + f.getName -> f).toMap
      .view.mapValues(f =>
        Using(Source.fromFile(f.toString)) { source => source.mkString }.get
      )
      .toMap
  }

  private def volumeName = prefix + "-volume"

  // Mount the config volume to the expected config directory
  def getVolumeMounts: Seq[VolumeMount] = {
    Seq(VolumeMount()
      .withName(volumeName)
      .withMountPath(REMOTE_CONF_DIR_NAME)
      .withReadOnly(true))
  }

  // Use the k8s downward api to map the config files to a volume
  def getVolumes: Seq[Volume] = {
    def getDownAPIVolumeFile(f: File) = {
      DownwardAPIVolumeFile()
        .withPath(f.getName)
        .withFieldRef(
          ObjectFieldSelector()
            .withFieldPath(s"metadata.annotations['$prefix/${f.getName}']"))
    }
    val downwardAPIVolumeFiles = confFiles.map(getDownAPIVolumeFile)
    val volumeSource = VolumeSource()
      .withDownwardAPI(DownwardAPIVolumeSource().withItems(downwardAPIVolumeFiles))
    Seq(Volume()
      .withName(volumeName)
      .withVolumeSource(volumeSource))
  }
}
