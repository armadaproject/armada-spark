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
package org.apache.spark.deploy.armada

import k8s.io.api.core.v1.generated.{DownwardAPIVolumeFile, DownwardAPIVolumeSource, ObjectFieldSelector, Volume, VolumeMount, VolumeSource}
import org.apache.spark.SparkConf
import org.apache.spark.deploy.k8s.Constants.ENV_SPARK_CONF_DIR

import java.io.File
import scala.io.Source


class ArmadaConfigGenerator(val confDirName: String, val prefix: String,
                            val conf: SparkConf) {
  private val confDir = Option(conf.getenv(ENV_SPARK_CONF_DIR)).orElse(
    conf.getOption("spark.home").map(dir => s"$dir/conf"))

  private val confFiles : Array[File] = {
    val dir = new File(confDir.get)
    if (dir.isDirectory) {
      dir.listFiles
    } else {
      Array.empty[File]
    }
  }

  def getAnnotations: Map[String, String] = {
    confFiles.map(f =>
      (prefix + "/" + f.getName, Source.fromFile(f.toString).mkString)).toMap
  }

  def getVolumeMounts: Seq[VolumeMount] = {
    Seq(VolumeMount()
      .withName(prefix + "-volume")
      .withMountPath(confDirName)
      .withReadOnly(true))
  }

  def getVolumes: Seq[Volume] = {
    val dFiles = confFiles.map(
      f => DownwardAPIVolumeFile()
        .withPath(f.getName)
        .withFieldRef(
          ObjectFieldSelector()
            .withFieldPath("metadata.annotations['" + prefix + "/" + f.getName + "']")))
    val volumeSource = VolumeSource()
      .withDownwardAPI(DownwardAPIVolumeSource().withItems(dFiles))
    Seq(Volume()
      .withName(prefix + "-volume")
      .withVolumeSource(volumeSource))
  }
}
