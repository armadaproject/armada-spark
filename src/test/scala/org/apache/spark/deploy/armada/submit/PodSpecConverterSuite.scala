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

import io.fabric8.kubernetes.api.model._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import scala.jdk.CollectionConverters._

class PodSpecConverterSuite extends AnyFunSuite with Matchers {

  test(
    "should convert fabric8 PodSpec to protobuf and back preserving all supported volume types"
  ) {
    val fabric8Spec = new PodSpec()

    // Add various volume types
    val volumes = new java.util.ArrayList[Volume]()

    val emptyDirVolume = new Volume()
    emptyDirVolume.setName("empty-dir-volume")
    val emptyDir = new EmptyDirVolumeSource()
    emptyDir.setMedium("Memory")
    emptyDir.setSizeLimit(new Quantity("1Gi"))
    emptyDirVolume.setEmptyDir(emptyDir)
    volumes.add(emptyDirVolume)

    val hostPathVolume = new Volume()
    hostPathVolume.setName("host-path-volume")
    val hostPath = new HostPathVolumeSource()
    hostPath.setPath("/data")
    hostPath.setType("Directory")
    hostPathVolume.setHostPath(hostPath)
    volumes.add(hostPathVolume)

    val secretVolume = new Volume()
    secretVolume.setName("secret-volume")
    val secret = new SecretVolumeSource()
    secret.setSecretName("my-secret")
    secret.setDefaultMode(420)
    secret.setOptional(true)
    secretVolume.setSecret(secret)
    volumes.add(secretVolume)

    val configMapVolume = new Volume()
    configMapVolume.setName("config-map-volume")
    val configMap = new ConfigMapVolumeSource()
    configMap.setName("my-config")
    configMap.setDefaultMode(420)
    configMap.setOptional(false)
    configMapVolume.setConfigMap(configMap)
    volumes.add(configMapVolume)

    val pvcVolume = new Volume()
    pvcVolume.setName("pvc-volume")
    val pvc = new PersistentVolumeClaimVolumeSource()
    pvc.setClaimName("my-claim")
    pvc.setReadOnly(true)
    pvcVolume.setPersistentVolumeClaim(pvc)
    volumes.add(pvcVolume)

    val nfsVolume = new Volume()
    nfsVolume.setName("nfs-volume")
    val nfs = new NFSVolumeSource()
    nfs.setServer("nfs.example.com")
    nfs.setPath("/exports/data")
    nfs.setReadOnly(false)
    nfsVolume.setNfs(nfs)
    volumes.add(nfsVolume)

    val csiVolume = new Volume()
    csiVolume.setName("csi-volume")
    val csi = new CSIVolumeSource()
    csi.setDriver("csi.example.com")
    csi.setFsType("ext4")
    csi.setReadOnly(false)
    val attrs = new java.util.HashMap[String, String]()
    attrs.put("key1", "value1")
    csi.setVolumeAttributes(attrs)
    csiVolume.setCsi(csi)
    volumes.add(csiVolume)

    val azureDiskVolume = new Volume()
    azureDiskVolume.setName("azure-disk-volume")
    val azureDisk = new AzureDiskVolumeSource()
    azureDisk.setDiskName("myDisk")
    azureDisk.setDiskURI("https://azure.com/disk")
    azureDisk.setFsType("ext4")
    azureDiskVolume.setAzureDisk(azureDisk)
    volumes.add(azureDiskVolume)

    val projectedVolume = new Volume()
    projectedVolume.setName("projected-volume")
    val projected = new ProjectedVolumeSource()
    projected.setDefaultMode(420)
    val sources = new java.util.ArrayList[VolumeProjection]()

    val secretProj       = new VolumeProjection()
    val secretProjSource = new SecretProjection()
    secretProjSource.setName("proj-secret")
    secretProj.setSecret(secretProjSource)
    sources.add(secretProj)

    val configMapProj       = new VolumeProjection()
    val configMapProjSource = new ConfigMapProjection()
    configMapProjSource.setName("proj-config")
    configMapProj.setConfigMap(configMapProjSource)
    sources.add(configMapProj)

    projected.setSources(sources)
    projectedVolume.setProjected(projected)
    volumes.add(projectedVolume)

    fabric8Spec.setVolumes(volumes)

    // Add some basic pod settings
    fabric8Spec.setHostname("test-host")
    fabric8Spec.setNodeName("test-node")
    fabric8Spec.setServiceAccountName("test-sa")

    val protobufSpec = PodSpecConverter.fabric8ToProtobuf(fabric8Spec)

    // Verify protobuf has all volumes
    protobufSpec.volumes should have size 9
    protobufSpec.hostname shouldBe Some("test-host")
    protobufSpec.nodeName shouldBe Some("test-node")
    protobufSpec.serviceAccountName shouldBe Some("test-sa")

    val fabric8SpecBack = PodSpecConverter.protobufToFabric8(protobufSpec)

    // Verify round-trip preserved all data
    fabric8SpecBack.getHostname shouldBe "test-host"
    fabric8SpecBack.getNodeName shouldBe "test-node"
    fabric8SpecBack.getServiceAccountName shouldBe "test-sa"

    val volumesBack = fabric8SpecBack.getVolumes.asScala
    volumesBack should have size 9

    // Verify each volume type converted correctly
    val emptyDirBack = volumesBack.find(_.getName == "empty-dir-volume").get
    emptyDirBack.getEmptyDir should not be null
    emptyDirBack.getEmptyDir.getMedium shouldBe "Memory"
    emptyDirBack.getEmptyDir.getSizeLimit should not be null

    val hostPathBack = volumesBack.find(_.getName == "host-path-volume").get
    hostPathBack.getHostPath should not be null
    hostPathBack.getHostPath.getPath shouldBe "/data"
    hostPathBack.getHostPath.getType shouldBe "Directory"

    val secretBack = volumesBack.find(_.getName == "secret-volume").get
    secretBack.getSecret should not be null
    secretBack.getSecret.getSecretName shouldBe "my-secret"
    secretBack.getSecret.getDefaultMode shouldBe 420
    secretBack.getSecret.getOptional shouldBe true

    val configMapBack = volumesBack.find(_.getName == "config-map-volume").get
    configMapBack.getConfigMap should not be null
    configMapBack.getConfigMap.getName shouldBe "my-config"
    configMapBack.getConfigMap.getDefaultMode shouldBe 420
    configMapBack.getConfigMap.getOptional shouldBe false

    val pvcBack = volumesBack.find(_.getName == "pvc-volume").get
    pvcBack.getPersistentVolumeClaim should not be null
    pvcBack.getPersistentVolumeClaim.getClaimName shouldBe "my-claim"
    pvcBack.getPersistentVolumeClaim.getReadOnly shouldBe true

    val nfsBack = volumesBack.find(_.getName == "nfs-volume").get
    nfsBack.getNfs should not be null
    nfsBack.getNfs.getServer shouldBe "nfs.example.com"
    nfsBack.getNfs.getPath shouldBe "/exports/data"
    nfsBack.getNfs.getReadOnly shouldBe false

    val csiBack = volumesBack.find(_.getName == "csi-volume").get
    csiBack.getCsi should not be null
    csiBack.getCsi.getDriver shouldBe "csi.example.com"
    csiBack.getCsi.getFsType shouldBe "ext4"
    csiBack.getCsi.getReadOnly shouldBe false
    csiBack.getCsi.getVolumeAttributes.asScala shouldBe Map("key1" -> "value1")

    val azureDiskBack = volumesBack.find(_.getName == "azure-disk-volume").get
    azureDiskBack.getAzureDisk should not be null
    azureDiskBack.getAzureDisk.getDiskName shouldBe "myDisk"
    azureDiskBack.getAzureDisk.getDiskURI shouldBe "https://azure.com/disk"
    azureDiskBack.getAzureDisk.getFsType shouldBe "ext4"

    val projectedBack = volumesBack.find(_.getName == "projected-volume").get
    projectedBack.getProjected should not be null
    projectedBack.getProjected.getDefaultMode shouldBe 420
    projectedBack.getProjected.getSources should have size 2
  }

  test("should handle null and empty values gracefully") {
    val fabric8Spec = new PodSpec()

    // Convert empty spec
    val protobufSpec = PodSpecConverter.fabric8ToProtobuf(fabric8Spec)
    protobufSpec should not be null

    // Convert back
    val fabric8SpecBack = PodSpecConverter.protobufToFabric8(protobufSpec)
    fabric8SpecBack should not be null
  }

  test("should convert containers with all fields") {
    val fabric8Spec = new PodSpec()

    val containers = new java.util.ArrayList[Container]()
    val container  = new Container()
    container.setName("test-container")
    container.setImage("test-image:latest")
    container.setImagePullPolicy("Always")
    container.setWorkingDir("/app")
    container.setArgs(List("arg1", "arg2").asJava)
    container.setCommand(List("/bin/sh", "-c").asJava)

    val ports = new java.util.ArrayList[ContainerPort]()
    val port  = new ContainerPort()
    port.setContainerPort(8080)
    port.setProtocol("TCP")
    port.setName("http")
    ports.add(port)
    container.setPorts(ports)

    val envVars = new java.util.ArrayList[EnvVar]()
    val envVar  = new EnvVar()
    envVar.setName("TEST_VAR")
    envVar.setValue("test-value")
    envVars.add(envVar)
    container.setEnv(envVars)

    val volumeMounts = new java.util.ArrayList[VolumeMount]()
    val mount        = new VolumeMount()
    mount.setName("test-volume")
    mount.setMountPath("/data")
    mount.setReadOnly(true)
    volumeMounts.add(mount)
    container.setVolumeMounts(volumeMounts)

    val resources = new ResourceRequirements()
    val limits    = new java.util.HashMap[String, Quantity]()
    limits.put("cpu", new Quantity("1"))
    limits.put("memory", new Quantity("1Gi"))
    resources.setLimits(limits)
    val requests = new java.util.HashMap[String, Quantity]()
    requests.put("cpu", new Quantity("500m"))
    requests.put("memory", new Quantity("512Mi"))
    resources.setRequests(requests)
    container.setResources(resources)

    containers.add(container)
    fabric8Spec.setContainers(containers)

    val protobufSpec = PodSpecConverter.fabric8ToProtobuf(fabric8Spec)
    protobufSpec.containers should have size 1

    val protobufContainer = protobufSpec.containers.head
    protobufContainer.name shouldBe Some("test-container")
    protobufContainer.image shouldBe Some("test-image:latest")
    protobufContainer.imagePullPolicy shouldBe Some("Always")
    protobufContainer.workingDir shouldBe Some("/app")
    protobufContainer.args should contain theSameElementsAs Seq("arg1", "arg2")
    protobufContainer.command should contain theSameElementsAs Seq("/bin/sh", "-c")
    protobufContainer.ports should have size 1
    protobufContainer.env should have size 1
    protobufContainer.volumeMounts should have size 1
    protobufContainer.resources should not be None

    val fabric8SpecBack = PodSpecConverter.protobufToFabric8(protobufSpec)
    val containerBack   = fabric8SpecBack.getContainers.get(0)

    containerBack.getName shouldBe "test-container"
    containerBack.getImage shouldBe "test-image:latest"
    containerBack.getImagePullPolicy shouldBe "Always"
    containerBack.getWorkingDir shouldBe "/app"
    containerBack.getArgs.asScala shouldBe List("arg1", "arg2")
    containerBack.getCommand.asScala shouldBe List("/bin/sh", "-c")
    containerBack.getPorts should have size 1
    containerBack.getEnv should have size 1
    containerBack.getVolumeMounts should have size 1
    containerBack.getResources.getLimits.asScala should have size 2
    containerBack.getResources.getRequests.asScala should have size 2
  }
}
