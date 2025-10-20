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

import io.fabric8.kubernetes.api.model.{
  ContainerBuilder,
  PodBuilder,
  Quantity,
  ResourceRequirements
}
import org.apache.spark.deploy.k8s.SparkPod
import org.apache.spark.deploy.k8s.features.KubernetesFeatureConfigStep
import java.util.HashMap

class DriverFeatureStep extends KubernetesFeatureConfigStep {

  override def configurePod(pod: SparkPod): SparkPod = {
    val resources = new ResourceRequirements()
    val limits    = new HashMap[String, Quantity]()
    limits.put("cpu", new Quantity("100m"))
    limits.put("memory", new Quantity("64Mi"))
    resources.setLimits(limits)

    val requests = new HashMap[String, Quantity]()
    requests.put("cpu", new Quantity("100m"))
    requests.put("memory", new Quantity("64Mi"))
    resources.setRequests(requests)

    val initContainer = new ContainerBuilder()
      .withName("driver-init")
      .withImage("alpine:latest")
      .withCommand("/bin/sh", "-c")
      .withArgs("echo 'Hello from driver init container!'")
      .withResources(resources)
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
      .endSpec()
      .build()

    SparkPod(configuredPod, pod.container)
  }
}
