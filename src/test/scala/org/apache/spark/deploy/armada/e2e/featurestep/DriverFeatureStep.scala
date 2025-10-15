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

package org.apache.spark.deploy.armada.e2e.featurestep

import io.fabric8.kubernetes.api.model._
import org.apache.spark.deploy.k8s.SparkPod
import org.apache.spark.deploy.k8s.features.KubernetesFeatureConfigStep

class DriverFeatureStep extends KubernetesFeatureConfigStep {

  override def configurePod(pod: SparkPod): SparkPod = {
    val initContainer = new ContainerBuilder()
      .withName("driver-init")
      .withImage("alpine:latest")
      .withCommand("/bin/sh", "-c")
      .withArgs("echo 'Hello from driver init container!'")
      .withNewResources()
      .addToRequests("cpu", new Quantity("100m"))
      .addToRequests("memory", new Quantity("128Mi"))
      .addToLimits("cpu", new Quantity("100m"))
      .addToLimits("memory", new Quantity("128Mi"))
      .endResources()
      .build()

    val configuredContainer = new ContainerBuilder(pod.container)
      .addNewEnv()
      .withName("DRIVER_FEATURE_STEP_ENV")
      .withValue("driver-feature-value")
      .endEnv()
      .addNewVolumeMount()
      .withName("driver-feature-volume")
      .withMountPath("/mnt/driver-feature")
      .endVolumeMount()
      .build()

    val configuredPod = new PodBuilder(pod.pod)
      .editOrNewMetadata()
      .addToLabels("feature-step", "driver-applied")
      .addToLabels("feature-step-role", "driver")
      .addToAnnotations("driver-feature-step", "configured")
      .endMetadata()
      .editOrNewSpec()
      .withActiveDeadlineSeconds(3600L)
      .addToInitContainers(initContainer)
      .addNewVolume()
      .withName("driver-feature-volume")
      .withNewEmptyDir()
      .endEmptyDir()
      .endVolume()
      .endSpec()
      .build()

    SparkPod(configuredPod, configuredContainer)
  }
}
