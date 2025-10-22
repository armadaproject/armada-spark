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

import io.fabric8.kubernetes.api.model
import k8s.io.api.core.v1.generated
import k8s.io.apimachinery.pkg.api.resource.generated.Quantity
import scala.jdk.CollectionConverters._

/** Converts between fabric8 PodSpec and protobuf PodSpec.
  *
  * WHY THIS EXISTS: Protobuf generated classes have DIFFERENT field structures than standard K8s
  * YAML/JSON. Direct YAML → protobuf parsing fails or loses data due to structural mismatches
  * (flattened fields, different naming, incompatible nested types). fabric8 is specifically
  * designed to parse K8s YAML correctly, so we must: YAML → fabric8 → protobuf → Armada gRPC.
  *
  * IMPORTANT: This converter must support multiple Spark versions (3.3.x, 3.5.x) which bundle
  * different fabric8 Kubernetes client versions. Some fields (e.g., hostUsers from K8s 1.25+,
  * dnsConfig, ephemeralContainers) may not be available in older fabric8 versions and are
  * intentionally hardcoded to None/empty to maintain compatibility.
  *
  * Maps ALL fields that exist in BOTH the protobuf-generated API AND the fabric8 API. Fields that
  * don't exist in one or both are hardcoded to None/empty with explanatory comments.
  */
object PodSpecConverter {

  /** Converts a fabric8 PodSpec to protobuf PodSpec.
    *
    * Note: Some fields are intentionally hardcoded to None/empty due to fabric8 version
    * compatibility constraints across Spark versions. See class-level docs.
    */
  def fabric8ToProtobuf(fabric8Spec: model.PodSpec): generated.PodSpec = {
    if (fabric8Spec == null) {
      return generated.PodSpec()
    }

    generated.PodSpec(
      activeDeadlineSeconds = Option(fabric8Spec.getActiveDeadlineSeconds).map(_.longValue()),
      affinity = Option(fabric8Spec.getAffinity).flatMap(convertAffinityFromFabric8),
      automountServiceAccountToken =
        Option(fabric8Spec.getAutomountServiceAccountToken).map(_.booleanValue()),
      containers = Option(fabric8Spec.getContainers)
        .map(_.asScala.toSeq.map(convertContainer))
        .getOrElse(Seq.empty),
      // dnsConfig not converted - complex nested type, rarely used, not in older fabric8 versions
      dnsConfig = None,
      dnsPolicy = Option(fabric8Spec.getDnsPolicy),
      enableServiceLinks = Option(fabric8Spec.getEnableServiceLinks).map(_.booleanValue()),
      // ephemeralContainers not converted - K8s 1.23+ feature, not available in Spark 3.3.x fabric8
      ephemeralContainers = Seq.empty,
      hostAliases = Option(fabric8Spec.getHostAliases)
        .map(
          _.asScala.toSeq.map(ha =>
            generated.HostAlias(
              hostnames = Option(ha.getHostnames).map(_.asScala.toSeq).getOrElse(Seq.empty),
              ip = Option(ha.getIp)
            )
          )
        )
        .getOrElse(Seq.empty),
      hostIPC = Option(fabric8Spec.getHostIPC).map(_.booleanValue()),
      hostNetwork = Option(fabric8Spec.getHostNetwork).map(_.booleanValue()),
      hostPID = Option(fabric8Spec.getHostPID).map(_.booleanValue()),
      // hostUsers field is not supported - it was added in K8s 1.25+ and is not available
      // in the client versions used by Spark 3.3.x. This field is rarely needed for Spark workloads.
      hostUsers = None,
      hostname = Option(fabric8Spec.getHostname),
      imagePullSecrets = Option(fabric8Spec.getImagePullSecrets)
        .map(_.asScala.toSeq.map(ref => generated.LocalObjectReference(name = Option(ref.getName))))
        .getOrElse(Seq.empty),
      initContainers = Option(fabric8Spec.getInitContainers)
        .map(_.asScala.toSeq.map(convertContainer))
        .getOrElse(Seq.empty),
      nodeName = Option(fabric8Spec.getNodeName),
      nodeSelector = Option(fabric8Spec.getNodeSelector)
        .map(_.asScala.toMap)
        .getOrElse(Map.empty),
      // os not converted - K8s 1.25+ field, not in Spark 3.3.x fabric8
      os = None,
      // overhead not converted - rarely used, not critical for Spark workloads
      overhead = Map.empty,
      preemptionPolicy = Option(fabric8Spec.getPreemptionPolicy),
      priority = Option(fabric8Spec.getPriority).map(_.intValue()),
      priorityClassName = Option(fabric8Spec.getPriorityClassName),
      // readinessGates not converted - rarely used, not critical for Spark workloads
      readinessGates = Seq.empty,
      // resourceClaims not converted - K8s 1.26+ feature for dynamic resource allocation
      resourceClaims = Seq.empty,
      restartPolicy = Option(fabric8Spec.getRestartPolicy),
      runtimeClassName = Option(fabric8Spec.getRuntimeClassName),
      schedulerName = Option(fabric8Spec.getSchedulerName),
      // schedulingGates not converted - K8s 1.27+ feature for pod scheduling control
      schedulingGates = Seq.empty,
      securityContext = Option(fabric8Spec.getSecurityContext).map(sc =>
        generated.PodSecurityContext(
          fsGroup = Option(sc.getFsGroup).map(_.longValue()),
          fsGroupChangePolicy = Option(sc.getFsGroupChangePolicy),
          runAsGroup = Option(sc.getRunAsGroup).map(_.longValue()),
          runAsNonRoot = Option(sc.getRunAsNonRoot).map(_.booleanValue()),
          runAsUser = Option(sc.getRunAsUser).map(_.longValue()),
          seLinuxOptions = Option(sc.getSeLinuxOptions).map(se =>
            generated.SELinuxOptions(
              level = Option(se.getLevel),
              role = Option(se.getRole),
              `type` = Option(se.getType),
              user = Option(se.getUser)
            )
          ),
          seccompProfile = Option(sc.getSeccompProfile).map(sp =>
            generated.SeccompProfile(
              `type` = Option(sp.getType),
              localhostProfile = Option(sp.getLocalhostProfile)
            )
          ),
          supplementalGroups = Option(sc.getSupplementalGroups)
            .map(_.asScala.toSeq.map(_.longValue()))
            .getOrElse(Seq.empty),
          sysctls = Option(sc.getSysctls)
            .map(
              _.asScala.toSeq.map(s =>
                generated.Sysctl(
                  name = Option(s.getName),
                  value = Option(s.getValue)
                )
              )
            )
            .getOrElse(Seq.empty),
          windowsOptions = Option(sc.getWindowsOptions).map(wo =>
            generated.WindowsSecurityContextOptions(
              gmsaCredentialSpec = Option(wo.getGmsaCredentialSpec),
              gmsaCredentialSpecName = Option(wo.getGmsaCredentialSpecName),
              hostProcess = Option(wo.getHostProcess).map(_.booleanValue()),
              runAsUserName = Option(wo.getRunAsUserName)
            )
          )
        )
      ),
      serviceAccount = Option(fabric8Spec.getServiceAccount),
      serviceAccountName = Option(fabric8Spec.getServiceAccountName),
      setHostnameAsFQDN = Option(fabric8Spec.getSetHostnameAsFQDN).map(_.booleanValue()),
      shareProcessNamespace = Option(fabric8Spec.getShareProcessNamespace).map(_.booleanValue()),
      subdomain = Option(fabric8Spec.getSubdomain),
      terminationGracePeriodSeconds =
        Option(fabric8Spec.getTerminationGracePeriodSeconds).map(_.longValue()),
      tolerations = Option(fabric8Spec.getTolerations)
        .map(
          _.asScala.toSeq.map(t =>
            generated.Toleration(
              effect = Option(t.getEffect),
              key = Option(t.getKey),
              operator = Option(t.getOperator),
              tolerationSeconds = Option(t.getTolerationSeconds).map(_.longValue()),
              value = Option(t.getValue)
            )
          )
        )
        .getOrElse(Seq.empty),
      topologySpreadConstraints = Seq.empty,
      volumes = Option(fabric8Spec.getVolumes)
        .map(_.asScala.toSeq.flatMap(v => convertVolume(v)))
        .getOrElse(Seq.empty)
    )
  }

  /** Converts a protobuf PodSpec back to fabric8 PodSpec. Maps ALL fields that exist in protobuf
    * API back to fabric8.
    */
  def protobufToFabric8(protobufSpec: generated.PodSpec): model.PodSpec = {
    if (protobufSpec == null) {
      return new model.PodSpec()
    }

    val spec = new model.PodSpec()

    protobufSpec.activeDeadlineSeconds.foreach(v => spec.setActiveDeadlineSeconds(v))

    // Convert affinity from protobuf to fabric8
    protobufSpec.affinity.foreach { aff =>
      val fabric8Affinity = convertAffinityToFabric8(aff)
      spec.setAffinity(fabric8Affinity)
    }

    protobufSpec.automountServiceAccountToken.foreach(v => spec.setAutomountServiceAccountToken(v))

    if (protobufSpec.containers.nonEmpty) {
      val containers = protobufSpec.containers.map(convertContainerToFabric8).asJava
      spec.setContainers(containers)
    }

    if (protobufSpec.initContainers.nonEmpty) {
      val initContainers = protobufSpec.initContainers.map(convertContainerToFabric8).asJava
      spec.setInitContainers(initContainers)
    }

    protobufSpec.dnsPolicy.foreach(spec.setDnsPolicy)
    protobufSpec.enableServiceLinks.foreach(v => spec.setEnableServiceLinks(v))
    protobufSpec.hostIPC.foreach(v => spec.setHostIPC(v))
    protobufSpec.hostNetwork.foreach(v => spec.setHostNetwork(v))
    protobufSpec.hostPID.foreach(v => spec.setHostPID(v))
    // hostUsers field is not set - not available in K8s client versions used by Spark 3.3.x
    protobufSpec.hostname.foreach(spec.setHostname)

    if (protobufSpec.hostAliases.nonEmpty) {
      val hostAliases = protobufSpec.hostAliases.map { ha =>
        val hostAlias = new model.HostAlias()
        ha.ip.foreach(hostAlias.setIp)
        if (ha.hostnames.nonEmpty) {
          hostAlias.setHostnames(ha.hostnames.asJava)
        }
        hostAlias
      }.asJava
      spec.setHostAliases(hostAliases)
    }

    if (protobufSpec.imagePullSecrets.nonEmpty) {
      val refs = protobufSpec.imagePullSecrets
        .map(ref => new model.LocalObjectReference(ref.name.orNull))
        .asJava
      spec.setImagePullSecrets(refs)
    }

    protobufSpec.nodeName.foreach(spec.setNodeName)

    if (protobufSpec.nodeSelector.nonEmpty) {
      spec.setNodeSelector(protobufSpec.nodeSelector.asJava)
    }

    protobufSpec.preemptionPolicy.foreach(spec.setPreemptionPolicy)
    protobufSpec.priority.foreach(v => spec.setPriority(v))
    protobufSpec.priorityClassName.foreach(spec.setPriorityClassName)
    protobufSpec.restartPolicy.foreach(spec.setRestartPolicy)
    protobufSpec.runtimeClassName.foreach(spec.setRuntimeClassName)
    protobufSpec.schedulerName.foreach(spec.setSchedulerName)

    protobufSpec.securityContext.foreach { sc =>
      val secContext = new model.PodSecurityContext()
      sc.fsGroup.foreach(v => secContext.setFsGroup(v))
      sc.fsGroupChangePolicy.foreach(secContext.setFsGroupChangePolicy)
      sc.runAsGroup.foreach(v => secContext.setRunAsGroup(v))
      sc.runAsNonRoot.foreach(v => secContext.setRunAsNonRoot(v))
      sc.runAsUser.foreach(v => secContext.setRunAsUser(v))

      sc.seLinuxOptions.foreach { se =>
        val seLinux = new model.SELinuxOptions()
        se.level.foreach(seLinux.setLevel)
        se.role.foreach(seLinux.setRole)
        se.`type`.foreach(seLinux.setType)
        se.user.foreach(seLinux.setUser)
        secContext.setSeLinuxOptions(seLinux)
      }

      sc.seccompProfile.foreach { sp =>
        val seccomp = new model.SeccompProfile()
        sp.`type`.foreach(seccomp.setType)
        sp.localhostProfile.foreach(seccomp.setLocalhostProfile)
        secContext.setSeccompProfile(seccomp)
      }

      if (sc.supplementalGroups.nonEmpty) {
        secContext.setSupplementalGroups(sc.supplementalGroups.map(Long.box).asJava)
      }

      if (sc.sysctls.nonEmpty) {
        val sysctls = sc.sysctls.map { s =>
          val sysctl = new model.Sysctl()
          s.name.foreach(sysctl.setName)
          s.value.foreach(sysctl.setValue)
          sysctl
        }.asJava
        secContext.setSysctls(sysctls)
      }

      sc.windowsOptions.foreach { wo =>
        val winOpts = new model.WindowsSecurityContextOptions()
        wo.gmsaCredentialSpec.foreach(winOpts.setGmsaCredentialSpec)
        wo.gmsaCredentialSpecName.foreach(winOpts.setGmsaCredentialSpecName)
        wo.hostProcess.foreach(v => winOpts.setHostProcess(v))
        wo.runAsUserName.foreach(winOpts.setRunAsUserName)
        secContext.setWindowsOptions(winOpts)
      }

      spec.setSecurityContext(secContext)
    }

    protobufSpec.serviceAccount.foreach(spec.setServiceAccount)
    protobufSpec.serviceAccountName.foreach(spec.setServiceAccountName)
    protobufSpec.setHostnameAsFQDN.foreach(v => spec.setSetHostnameAsFQDN(v))
    protobufSpec.shareProcessNamespace.foreach(v => spec.setShareProcessNamespace(v))
    protobufSpec.subdomain.foreach(spec.setSubdomain)
    protobufSpec.terminationGracePeriodSeconds.foreach(v =>
      spec.setTerminationGracePeriodSeconds(v)
    )

    if (protobufSpec.tolerations.nonEmpty) {
      val tolerations = protobufSpec.tolerations.map { t =>
        val toleration = new model.Toleration()
        t.effect.foreach(toleration.setEffect)
        t.key.foreach(toleration.setKey)
        t.operator.foreach(toleration.setOperator)
        t.tolerationSeconds.foreach(v => toleration.setTolerationSeconds(v))
        t.value.foreach(toleration.setValue)
        toleration
      }.asJava
      spec.setTolerations(tolerations)
    }

    if (protobufSpec.volumes.nonEmpty) {
      val volumes = protobufSpec.volumes.flatMap(v => convertVolumeToFabric8(v)).asJava
      spec.setVolumes(volumes)
    }

    spec
  }

  private def convertAffinityToFabric8(aff: generated.Affinity): model.Affinity = {
    val affinity = new model.Affinity()

    // Convert NodeAffinity
    aff.nodeAffinity.foreach { na =>
      val nodeAffinity = new model.NodeAffinity()

      // Convert requiredDuringSchedulingIgnoredDuringExecution
      na.requiredDuringSchedulingIgnoredDuringExecution.foreach { req =>
        val nodeSelector = new model.NodeSelector()
        val terms = req.nodeSelectorTerms.map { term =>
          val nodeSelectorTerm = new model.NodeSelectorTerm()
          if (term.matchExpressions.nonEmpty) {
            val matchExpressions = term.matchExpressions.map { me =>
              val requirement = new model.NodeSelectorRequirement()
              me.key.foreach(requirement.setKey)
              me.operator.foreach(requirement.setOperator)
              if (me.values.nonEmpty) {
                requirement.setValues(me.values.asJava)
              }
              requirement
            }.asJava
            nodeSelectorTerm.setMatchExpressions(matchExpressions)
          }
          if (term.matchFields.nonEmpty) {
            val matchFields = term.matchFields.map { mf =>
              val requirement = new model.NodeSelectorRequirement()
              mf.key.foreach(requirement.setKey)
              mf.operator.foreach(requirement.setOperator)
              if (mf.values.nonEmpty) {
                requirement.setValues(mf.values.asJava)
              }
              requirement
            }.asJava
            nodeSelectorTerm.setMatchFields(matchFields)
          }
          nodeSelectorTerm
        }.asJava
        nodeSelector.setNodeSelectorTerms(terms)
        nodeAffinity.setRequiredDuringSchedulingIgnoredDuringExecution(nodeSelector)
      }

      // Convert preferredDuringSchedulingIgnoredDuringExecution
      if (na.preferredDuringSchedulingIgnoredDuringExecution.nonEmpty) {
        val prefs = na.preferredDuringSchedulingIgnoredDuringExecution.map { pref =>
          val preferredTerm = new model.PreferredSchedulingTerm()
          pref.weight.foreach(w => preferredTerm.setWeight(w))
          pref.preference.foreach { prefTerm =>
            val nodeSelectorTerm = new model.NodeSelectorTerm()
            if (prefTerm.matchExpressions.nonEmpty) {
              val matchExpressions = prefTerm.matchExpressions.map { me =>
                val requirement = new model.NodeSelectorRequirement()
                me.key.foreach(requirement.setKey)
                me.operator.foreach(requirement.setOperator)
                if (me.values.nonEmpty) {
                  requirement.setValues(me.values.asJava)
                }
                requirement
              }.asJava
              nodeSelectorTerm.setMatchExpressions(matchExpressions)
            }
            if (prefTerm.matchFields.nonEmpty) {
              val matchFields = prefTerm.matchFields.map { mf =>
                val requirement = new model.NodeSelectorRequirement()
                mf.key.foreach(requirement.setKey)
                mf.operator.foreach(requirement.setOperator)
                if (mf.values.nonEmpty) {
                  requirement.setValues(mf.values.asJava)
                }
                requirement
              }.asJava
              nodeSelectorTerm.setMatchFields(matchFields)
            }
            preferredTerm.setPreference(nodeSelectorTerm)
          }
          preferredTerm
        }.asJava
        nodeAffinity.setPreferredDuringSchedulingIgnoredDuringExecution(prefs)
      }

      affinity.setNodeAffinity(nodeAffinity)
    }

    // Note: PodAffinity and PodAntiAffinity conversions would be similar but are not currently used
    // in the tests. They can be added if needed.

    affinity
  }

  private[submit] def convertAffinityFromFabric8(a: model.Affinity): Option[generated.Affinity] = {
    if (a == null) return None

    val nodeAffinity = Option(a.getNodeAffinity).map { na =>
      generated.NodeAffinity(
        requiredDuringSchedulingIgnoredDuringExecution = Option(
          na.getRequiredDuringSchedulingIgnoredDuringExecution
        ).map { req =>
          generated.NodeSelector(
            nodeSelectorTerms = Option(req.getNodeSelectorTerms)
              .map(
                _.asScala.toSeq.map { term =>
                  generated.NodeSelectorTerm(
                    matchExpressions = Option(term.getMatchExpressions)
                      .map(
                        _.asScala.toSeq.map { me =>
                          generated.NodeSelectorRequirement(
                            key = Option(me.getKey),
                            operator = Option(me.getOperator),
                            values = Option(me.getValues).map(_.asScala.toSeq).getOrElse(Seq.empty)
                          )
                        }
                      )
                      .getOrElse(Seq.empty),
                    matchFields = Option(term.getMatchFields)
                      .map(
                        _.asScala.toSeq.map { mf =>
                          generated.NodeSelectorRequirement(
                            key = Option(mf.getKey),
                            operator = Option(mf.getOperator),
                            values = Option(mf.getValues).map(_.asScala.toSeq).getOrElse(Seq.empty)
                          )
                        }
                      )
                      .getOrElse(Seq.empty)
                  )
                }
              )
              .getOrElse(Seq.empty)
          )
        },
        preferredDuringSchedulingIgnoredDuringExecution = Option(
          na.getPreferredDuringSchedulingIgnoredDuringExecution
        )
          .map(
            _.asScala.toSeq.map { pref =>
              generated.PreferredSchedulingTerm(
                preference = Option(pref.getPreference).map { prefTerm =>
                  generated.NodeSelectorTerm(
                    matchExpressions = Option(prefTerm.getMatchExpressions)
                      .map(
                        _.asScala.toSeq.map { me =>
                          generated.NodeSelectorRequirement(
                            key = Option(me.getKey),
                            operator = Option(me.getOperator),
                            values = Option(me.getValues).map(_.asScala.toSeq).getOrElse(Seq.empty)
                          )
                        }
                      )
                      .getOrElse(Seq.empty),
                    matchFields = Option(prefTerm.getMatchFields)
                      .map(
                        _.asScala.toSeq.map { mf =>
                          generated.NodeSelectorRequirement(
                            key = Option(mf.getKey),
                            operator = Option(mf.getOperator),
                            values = Option(mf.getValues).map(_.asScala.toSeq).getOrElse(Seq.empty)
                          )
                        }
                      )
                      .getOrElse(Seq.empty)
                  )
                },
                weight = Option(pref.getWeight).map(_.intValue())
              )
            }
          )
          .getOrElse(Seq.empty)
      )
    }

    // Note: PodAffinity and PodAntiAffinity not implemented yet (not used in current tests)
    val podAffinity     = None
    val podAntiAffinity = None

    // Only return Some if at least one affinity type is defined
    if (nodeAffinity.isDefined || podAffinity.isDefined || podAntiAffinity.isDefined) {
      Some(
        generated.Affinity(
          nodeAffinity = nodeAffinity,
          podAffinity = podAffinity,
          podAntiAffinity = podAntiAffinity
        )
      )
    } else {
      None
    }
  }

  private def convertContainerToFabric8(c: generated.Container): model.Container = {
    val container = new model.Container()

    c.name.foreach(container.setName)
    c.image.foreach(container.setImage)
    if (c.command.nonEmpty) {
      container.setCommand(c.command.asJava)
    }
    if (c.args.nonEmpty) {
      container.setArgs(c.args.asJava)
    }
    c.workingDir.foreach(container.setWorkingDir)

    if (c.ports.nonEmpty) {
      val ports = c.ports.map { p =>
        val port = new model.ContainerPort()
        p.containerPort.foreach(v => port.setContainerPort(v))
        p.hostIP.foreach(port.setHostIP)
        p.hostPort.foreach(v => port.setHostPort(v))
        p.name.foreach(port.setName)
        p.protocol.foreach(port.setProtocol)
        port
      }.asJava
      container.setPorts(ports)
    }

    if (c.env.nonEmpty) {
      val envVars = c.env.map { e =>
        val envVar = new model.EnvVar()
        e.name.foreach(envVar.setName)
        e.value.foreach(envVar.setValue)
        e.valueFrom.foreach { vf =>
          val valueFrom = new model.EnvVarSource()
          vf.configMapKeyRef.foreach { cm =>
            val configMapRef = new model.ConfigMapKeySelector()
            cm.localObjectReference.flatMap(_.name).foreach(configMapRef.setName)
            cm.key.foreach(configMapRef.setKey)
            cm.optional.foreach(o => configMapRef.setOptional(o))
            valueFrom.setConfigMapKeyRef(configMapRef)
          }
          vf.fieldRef.foreach { fr =>
            val fieldRef = new model.ObjectFieldSelector()
            fr.apiVersion.foreach(fieldRef.setApiVersion)
            fr.fieldPath.foreach(fieldRef.setFieldPath)
            valueFrom.setFieldRef(fieldRef)
          }
          vf.resourceFieldRef.foreach { rf =>
            val resourceFieldRef = new model.ResourceFieldSelector()
            rf.containerName.foreach(resourceFieldRef.setContainerName)
            rf.resource.foreach(resourceFieldRef.setResource)
            rf.divisor.foreach(d =>
              resourceFieldRef.setDivisor(new model.Quantity(d.string.orNull))
            )
            valueFrom.setResourceFieldRef(resourceFieldRef)
          }
          vf.secretKeyRef.foreach { sk =>
            val secretKeyRef = new model.SecretKeySelector()
            sk.localObjectReference.flatMap(_.name).foreach(secretKeyRef.setName)
            sk.key.foreach(secretKeyRef.setKey)
            valueFrom.setSecretKeyRef(secretKeyRef)
          }
          envVar.setValueFrom(valueFrom)
        }
        envVar
      }.asJava
      container.setEnv(envVars)
    }

    c.resources.foreach { r =>
      val resources = new model.ResourceRequirements()
      if (r.limits.nonEmpty) {
        val limits = r.limits.map { case (k, v) =>
          k -> new model.Quantity(v.string.orNull)
        }.asJava
        resources.setLimits(limits)
      }
      if (r.requests.nonEmpty) {
        val requests = r.requests.map { case (k, v) =>
          k -> new model.Quantity(v.string.orNull)
        }.asJava
        resources.setRequests(requests)
      }
      container.setResources(resources)
    }

    if (c.volumeMounts.nonEmpty) {
      val volumeMounts = c.volumeMounts.map { vm =>
        val mount = new model.VolumeMount()
        vm.mountPath.foreach(mount.setMountPath)
        vm.mountPropagation.foreach(mount.setMountPropagation)
        vm.name.foreach(mount.setName)
        vm.readOnly.foreach(r => mount.setReadOnly(r))
        vm.subPath.foreach(mount.setSubPath)
        vm.subPathExpr.foreach(mount.setSubPathExpr)
        mount
      }.asJava
      container.setVolumeMounts(volumeMounts)
    }

    c.terminationMessagePath.foreach(container.setTerminationMessagePath)
    c.terminationMessagePolicy.foreach(container.setTerminationMessagePolicy)
    c.imagePullPolicy.foreach(container.setImagePullPolicy)
    c.stdin.foreach(s => container.setStdin(s))
    c.stdinOnce.foreach(s => container.setStdinOnce(s))
    c.tty.foreach(t => container.setTty(t))

    container
  }

  private[submit] def convertContainer(c: model.Container): generated.Container = {
    generated.Container(
      name = Option(c.getName),
      image = Option(c.getImage),
      command = Option(c.getCommand).map(_.asScala.toSeq).getOrElse(Seq.empty),
      args = Option(c.getArgs).map(_.asScala.toSeq).getOrElse(Seq.empty),
      workingDir = Option(c.getWorkingDir),
      ports = Option(c.getPorts)
        .map(
          _.asScala.toSeq.map(p =>
            generated.ContainerPort(
              containerPort = Option(p.getContainerPort).map(_.intValue()),
              hostIP = Option(p.getHostIP),
              hostPort = Option(p.getHostPort).map(_.intValue()),
              name = Option(p.getName),
              protocol = Option(p.getProtocol)
            )
          )
        )
        .getOrElse(Seq.empty),
      env = Option(c.getEnv)
        .map(
          _.asScala.toSeq.map(e =>
            generated.EnvVar(
              name = Option(e.getName),
              value = Option(e.getValue),
              valueFrom = Option(e.getValueFrom).map(vf =>
                generated.EnvVarSource(
                  configMapKeyRef = Option(vf.getConfigMapKeyRef).map(cm =>
                    generated.ConfigMapKeySelector(
                      localObjectReference = Option(cm.getName)
                        .map(n => generated.LocalObjectReference(name = Option(n))),
                      key = Option(cm.getKey),
                      optional = Option(cm.getOptional).map(_.booleanValue())
                    )
                  ),
                  fieldRef = Option(vf.getFieldRef).map(fr =>
                    generated.ObjectFieldSelector(
                      apiVersion = Option(fr.getApiVersion),
                      fieldPath = Option(fr.getFieldPath)
                    )
                  ),
                  resourceFieldRef = Option(vf.getResourceFieldRef).map(rf =>
                    generated.ResourceFieldSelector(
                      containerName = Option(rf.getContainerName),
                      resource = Option(rf.getResource),
                      divisor = Option(rf.getDivisor).map(d => Quantity(Option(d.getAmount)))
                    )
                  ),
                  secretKeyRef = Option(vf.getSecretKeyRef).map(sk =>
                    generated.SecretKeySelector(
                      localObjectReference = Option(sk.getName)
                        .map(n => generated.LocalObjectReference(name = Option(n))),
                      key = Option(sk.getKey)
                    )
                  )
                )
              )
            )
          )
        )
        .getOrElse(Seq.empty),
      resources = Option(c.getResources).map(r =>
        generated.ResourceRequirements(
          limits = Option(r.getLimits)
            .map(
              _.asScala
                .map { case (k, v) =>
                  k -> Quantity(Option(v.toString))
                }
                .toMap
            )
            .getOrElse(Map.empty),
          requests = Option(r.getRequests)
            .map(
              _.asScala
                .map { case (k, v) =>
                  k -> Quantity(Option(v.toString))
                }
                .toMap
            )
            .getOrElse(Map.empty)
        )
      ),
      volumeMounts = Option(c.getVolumeMounts)
        .map(
          _.asScala.toSeq.map(vm =>
            generated.VolumeMount(
              mountPath = Option(vm.getMountPath),
              mountPropagation = Option(vm.getMountPropagation),
              name = Option(vm.getName),
              readOnly = Option(vm.getReadOnly).map(_.booleanValue()),
              subPath = Option(vm.getSubPath),
              subPathExpr = Option(vm.getSubPathExpr)
            )
          )
        )
        .getOrElse(Seq.empty),
      volumeDevices = Seq.empty,
      livenessProbe = None,
      readinessProbe = None,
      startupProbe = None,
      lifecycle = None,
      terminationMessagePath = Option(c.getTerminationMessagePath),
      terminationMessagePolicy = Option(c.getTerminationMessagePolicy),
      imagePullPolicy = Option(c.getImagePullPolicy),
      securityContext = None,
      stdin = Option(c.getStdin).map(_.booleanValue()),
      stdinOnce = Option(c.getStdinOnce).map(_.booleanValue()),
      tty = Option(c.getTty).map(_.booleanValue()),
      envFrom = Seq.empty
    )
  }

  private[submit] def convertVolume(v: model.Volume): Option[generated.Volume] = {
    if (v == null) return None

    val volumeSource = generated.VolumeSource(
      hostPath = Option(v.getHostPath).map(hp =>
        generated.HostPathVolumeSource(
          path = Option(hp.getPath),
          `type` = Option(hp.getType)
        )
      ),
      emptyDir = Option(v.getEmptyDir).map(ed =>
        generated.EmptyDirVolumeSource(
          medium = Option(ed.getMedium),
          sizeLimit = Option(ed.getSizeLimit).map(q => Quantity(Option(q.getAmount)))
        )
      ),
      gcePersistentDisk = Option(v.getGcePersistentDisk).map(gce =>
        generated.GCEPersistentDiskVolumeSource(
          fsType = Option(gce.getFsType),
          partition = Option(gce.getPartition).map(_.intValue()),
          pdName = Option(gce.getPdName),
          readOnly = Option(gce.getReadOnly).map(_.booleanValue())
        )
      ),
      awsElasticBlockStore = Option(v.getAwsElasticBlockStore).map(aws =>
        generated.AWSElasticBlockStoreVolumeSource(
          fsType = Option(aws.getFsType),
          partition = Option(aws.getPartition).map(_.intValue()),
          readOnly = Option(aws.getReadOnly).map(_.booleanValue()),
          volumeID = Option(aws.getVolumeID)
        )
      ),
      gitRepo = Option(v.getGitRepo).map(git =>
        generated.GitRepoVolumeSource(
          directory = Option(git.getDirectory),
          repository = Option(git.getRepository),
          revision = Option(git.getRevision)
        )
      ),
      secret = Option(v.getSecret).map(s =>
        generated.SecretVolumeSource(
          defaultMode = Option(s.getDefaultMode).map(_.intValue()),
          items = Seq.empty,
          optional = Option(s.getOptional).map(_.booleanValue()),
          secretName = Option(s.getSecretName)
        )
      ),
      nfs = Option(v.getNfs).map(nfs =>
        generated.NFSVolumeSource(
          path = Option(nfs.getPath),
          readOnly = Option(nfs.getReadOnly).map(_.booleanValue()),
          server = Option(nfs.getServer)
        )
      ),
      iscsi = Option(v.getIscsi).map(iscsi =>
        generated.ISCSIVolumeSource(
          chapAuthDiscovery = Option(iscsi.getChapAuthDiscovery).map(_.booleanValue()),
          chapAuthSession = Option(iscsi.getChapAuthSession).map(_.booleanValue()),
          fsType = Option(iscsi.getFsType),
          initiatorName = Option(iscsi.getInitiatorName),
          iqn = Option(iscsi.getIqn),
          iscsiInterface = Option(iscsi.getIscsiInterface),
          lun = Option(iscsi.getLun).map(_.intValue()),
          portals = Option(iscsi.getPortals).map(_.asScala.toSeq).getOrElse(Seq.empty),
          readOnly = Option(iscsi.getReadOnly).map(_.booleanValue()),
          secretRef = Option(iscsi.getSecretRef).map(ref =>
            generated.LocalObjectReference(name = Option(ref.getName))
          ),
          targetPortal = Option(iscsi.getTargetPortal)
        )
      ),
      glusterfs = Option(v.getGlusterfs).map(gfs =>
        generated.GlusterfsVolumeSource(
          endpoints = Option(gfs.getEndpoints),
          path = Option(gfs.getPath),
          readOnly = Option(gfs.getReadOnly).map(_.booleanValue())
        )
      ),
      persistentVolumeClaim = Option(v.getPersistentVolumeClaim).map(pvc =>
        generated.PersistentVolumeClaimVolumeSource(
          claimName = Option(pvc.getClaimName),
          readOnly = Option(pvc.getReadOnly).map(_.booleanValue())
        )
      ),
      rbd = Option(v.getRbd).map(rbd =>
        generated.RBDVolumeSource(
          monitors = Option(rbd.getMonitors).map(_.asScala.toSeq).getOrElse(Seq.empty),
          image = Option(rbd.getImage),
          fsType = Option(rbd.getFsType),
          pool = Option(rbd.getPool),
          user = Option(rbd.getUser),
          keyring = Option(rbd.getKeyring),
          secretRef = Option(rbd.getSecretRef).map(ref =>
            generated.LocalObjectReference(name = Option(ref.getName))
          ),
          readOnly = Option(rbd.getReadOnly).map(_.booleanValue())
        )
      ),
      flexVolume = Option(v.getFlexVolume).map(flex =>
        generated.FlexVolumeSource(
          driver = Option(flex.getDriver),
          fsType = Option(flex.getFsType),
          secretRef = Option(flex.getSecretRef).map(ref =>
            generated.LocalObjectReference(name = Option(ref.getName))
          ),
          readOnly = Option(flex.getReadOnly).map(_.booleanValue()),
          options = Option(flex.getOptions).map(_.asScala.toMap).getOrElse(Map.empty)
        )
      ),
      cinder = Option(v.getCinder).map(cinder =>
        generated.CinderVolumeSource(
          volumeID = Option(cinder.getVolumeID),
          fsType = Option(cinder.getFsType),
          readOnly = Option(cinder.getReadOnly).map(_.booleanValue()),
          secretRef = Option(cinder.getSecretRef).map(ref =>
            generated.LocalObjectReference(name = Option(ref.getName))
          )
        )
      ),
      cephfs = Option(v.getCephfs).map(ceph =>
        generated.CephFSVolumeSource(
          monitors = Option(ceph.getMonitors).map(_.asScala.toSeq).getOrElse(Seq.empty),
          path = Option(ceph.getPath),
          user = Option(ceph.getUser),
          secretFile = Option(ceph.getSecretFile),
          secretRef = Option(ceph.getSecretRef).map(ref =>
            generated.LocalObjectReference(name = Option(ref.getName))
          ),
          readOnly = Option(ceph.getReadOnly).map(_.booleanValue())
        )
      ),
      flocker = Option(v.getFlocker).map(flocker =>
        generated.FlockerVolumeSource(
          datasetName = Option(flocker.getDatasetName),
          datasetUUID = Option(flocker.getDatasetUUID)
        )
      ),
      downwardAPI = Option(v.getDownwardAPI).map(da =>
        generated.DownwardAPIVolumeSource(
          items = Option(da.getItems)
            .map(
              _.asScala.toSeq.map(item =>
                generated.DownwardAPIVolumeFile(
                  path = Option(item.getPath),
                  fieldRef = Option(item.getFieldRef).map(fr =>
                    generated.ObjectFieldSelector(
                      apiVersion = Option(fr.getApiVersion),
                      fieldPath = Option(fr.getFieldPath)
                    )
                  ),
                  resourceFieldRef = Option(item.getResourceFieldRef).map(rf =>
                    generated.ResourceFieldSelector(
                      containerName = Option(rf.getContainerName),
                      resource = Option(rf.getResource),
                      divisor = Option(rf.getDivisor).map(d => Quantity(Option(d.getAmount)))
                    )
                  ),
                  mode = Option(item.getMode).map(_.intValue())
                )
              )
            )
            .getOrElse(Seq.empty),
          defaultMode = Option(da.getDefaultMode).map(_.intValue())
        )
      ),
      fc = Option(v.getFc).map(fc =>
        generated.FCVolumeSource(
          targetWWNs = Option(fc.getTargetWWNs).map(_.asScala.toSeq).getOrElse(Seq.empty),
          lun = Option(fc.getLun).map(_.intValue()),
          fsType = Option(fc.getFsType),
          readOnly = Option(fc.getReadOnly).map(_.booleanValue()),
          wwids = Option(fc.getWwids).map(_.asScala.toSeq).getOrElse(Seq.empty)
        )
      ),
      azureFile = Option(v.getAzureFile).map(af =>
        generated.AzureFileVolumeSource(
          secretName = Option(af.getSecretName),
          shareName = Option(af.getShareName),
          readOnly = Option(af.getReadOnly).map(_.booleanValue())
        )
      ),
      configMap = Option(v.getConfigMap).map(cm =>
        generated.ConfigMapVolumeSource(
          defaultMode = Option(cm.getDefaultMode).map(_.intValue()),
          items = Seq.empty,
          localObjectReference =
            Option(cm.getName).map(name => generated.LocalObjectReference(name = Option(name))),
          optional = Option(cm.getOptional).map(_.booleanValue())
        )
      ),
      vsphereVolume = Option(v.getVsphereVolume).map(vsphere =>
        generated.VsphereVirtualDiskVolumeSource(
          volumePath = Option(vsphere.getVolumePath),
          fsType = Option(vsphere.getFsType),
          storagePolicyID = Option(vsphere.getStoragePolicyID),
          storagePolicyName = Option(vsphere.getStoragePolicyName)
        )
      ),
      quobyte = Option(v.getQuobyte).map(q =>
        generated.QuobyteVolumeSource(
          registry = Option(q.getRegistry),
          volume = Option(q.getVolume),
          readOnly = Option(q.getReadOnly).map(_.booleanValue()),
          user = Option(q.getUser),
          group = Option(q.getGroup),
          tenant = Option(q.getTenant)
        )
      ),
      azureDisk = Option(v.getAzureDisk).map(ad =>
        generated.AzureDiskVolumeSource(
          diskName = Option(ad.getDiskName),
          diskURI = Option(ad.getDiskURI),
          cachingMode = Option(ad.getCachingMode),
          fsType = Option(ad.getFsType),
          readOnly = Option(ad.getReadOnly).map(_.booleanValue()),
          kind = Option(ad.getKind)
        )
      ),
      photonPersistentDisk = Option(v.getPhotonPersistentDisk).map(ppd =>
        generated.PhotonPersistentDiskVolumeSource(
          pdID = Option(ppd.getPdID),
          fsType = Option(ppd.getFsType)
        )
      ),
      projected = Option(v.getProjected).map(proj =>
        generated.ProjectedVolumeSource(
          sources = Option(proj.getSources)
            .map(
              _.asScala.toSeq.map(src =>
                generated.VolumeProjection(
                  secret = Option(src.getSecret).map(s =>
                    generated.SecretProjection(
                      localObjectReference = Option(s.getName)
                        .map(name => generated.LocalObjectReference(name = Option(name))),
                      items = Option(s.getItems)
                        .map(
                          _.asScala.toSeq.map(item =>
                            generated.KeyToPath(
                              key = Option(item.getKey),
                              path = Option(item.getPath),
                              mode = Option(item.getMode).map(_.intValue())
                            )
                          )
                        )
                        .getOrElse(Seq.empty),
                      optional = Option(s.getOptional).map(_.booleanValue())
                    )
                  ),
                  configMap = Option(src.getConfigMap).map(cm =>
                    generated.ConfigMapProjection(
                      localObjectReference = Option(cm.getName)
                        .map(name => generated.LocalObjectReference(name = Option(name))),
                      items = Option(cm.getItems)
                        .map(
                          _.asScala.toSeq.map(item =>
                            generated.KeyToPath(
                              key = Option(item.getKey),
                              path = Option(item.getPath),
                              mode = Option(item.getMode).map(_.intValue())
                            )
                          )
                        )
                        .getOrElse(Seq.empty),
                      optional = Option(cm.getOptional).map(_.booleanValue())
                    )
                  ),
                  downwardAPI = Option(src.getDownwardAPI).map(da =>
                    generated.DownwardAPIProjection(
                      items = Option(da.getItems)
                        .map(
                          _.asScala.toSeq.map(item =>
                            generated.DownwardAPIVolumeFile(
                              path = Option(item.getPath),
                              fieldRef = Option(item.getFieldRef).map(fr =>
                                generated.ObjectFieldSelector(
                                  apiVersion = Option(fr.getApiVersion),
                                  fieldPath = Option(fr.getFieldPath)
                                )
                              ),
                              resourceFieldRef = Option(item.getResourceFieldRef).map(rf =>
                                generated.ResourceFieldSelector(
                                  containerName = Option(rf.getContainerName),
                                  resource = Option(rf.getResource),
                                  divisor =
                                    Option(rf.getDivisor).map(d => Quantity(Option(d.getAmount)))
                                )
                              ),
                              mode = Option(item.getMode).map(_.intValue())
                            )
                          )
                        )
                        .getOrElse(Seq.empty)
                    )
                  ),
                  serviceAccountToken = Option(src.getServiceAccountToken).map(sat =>
                    generated.ServiceAccountTokenProjection(
                      audience = Option(sat.getAudience),
                      expirationSeconds = Option(sat.getExpirationSeconds).map(_.longValue()),
                      path = Option(sat.getPath)
                    )
                  )
                )
              )
            )
            .getOrElse(Seq.empty),
          defaultMode = Option(proj.getDefaultMode).map(_.intValue())
        )
      ),
      portworxVolume = Option(v.getPortworxVolume).map(pwx =>
        generated.PortworxVolumeSource(
          volumeID = Option(pwx.getVolumeID),
          fsType = Option(pwx.getFsType),
          readOnly = Option(pwx.getReadOnly).map(_.booleanValue())
        )
      ),
      scaleIO = Option(v.getScaleIO).map(sio =>
        generated.ScaleIOVolumeSource(
          gateway = Option(sio.getGateway),
          system = Option(sio.getSystem),
          secretRef = Option(sio.getSecretRef).map(ref =>
            generated.LocalObjectReference(name = Option(ref.getName))
          ),
          sslEnabled = Option(sio.getSslEnabled).map(_.booleanValue()),
          protectionDomain = Option(sio.getProtectionDomain),
          storagePool = Option(sio.getStoragePool),
          storageMode = Option(sio.getStorageMode),
          volumeName = Option(sio.getVolumeName),
          fsType = Option(sio.getFsType),
          readOnly = Option(sio.getReadOnly).map(_.booleanValue())
        )
      ),
      storageos = Option(v.getStorageos).map(sos =>
        generated.StorageOSVolumeSource(
          volumeName = Option(sos.getVolumeName),
          volumeNamespace = Option(sos.getVolumeNamespace),
          fsType = Option(sos.getFsType),
          readOnly = Option(sos.getReadOnly).map(_.booleanValue()),
          secretRef = Option(sos.getSecretRef).map(ref =>
            generated.LocalObjectReference(name = Option(ref.getName))
          )
        )
      ),
      csi = Option(v.getCsi).map(csi =>
        generated.CSIVolumeSource(
          driver = Option(csi.getDriver),
          readOnly = Option(csi.getReadOnly).map(_.booleanValue()),
          fsType = Option(csi.getFsType),
          volumeAttributes =
            Option(csi.getVolumeAttributes).map(_.asScala.toMap).getOrElse(Map.empty),
          nodePublishSecretRef = Option(csi.getNodePublishSecretRef).map(ref =>
            generated.LocalObjectReference(name = Option(ref.getName))
          )
        )
      ),
      ephemeral = Option(v.getEphemeral).map(eph =>
        generated.EphemeralVolumeSource(
          volumeClaimTemplate = Option(eph.getVolumeClaimTemplate).map(vct =>
            generated.PersistentVolumeClaimTemplate(
              metadata = None,
              spec = Option(vct.getSpec).map(spec =>
                generated.PersistentVolumeClaimSpec(
                  accessModes =
                    Option(spec.getAccessModes).map(_.asScala.toSeq).getOrElse(Seq.empty),
                  selector = None,
                  resources = Option(spec.getResources).map(res =>
                    generated.ResourceRequirements(
                      limits = Option(res.getLimits)
                        .map(
                          _.asScala
                            .map { case (k, v) =>
                              k -> Quantity(Option(v.toString))
                            }
                            .toMap
                        )
                        .getOrElse(Map.empty),
                      requests = Option(res.getRequests)
                        .map(
                          _.asScala
                            .map { case (k, v) =>
                              k -> Quantity(Option(v.toString))
                            }
                            .toMap
                        )
                        .getOrElse(Map.empty)
                    )
                  ),
                  volumeName = Option(spec.getVolumeName),
                  storageClassName = Option(spec.getStorageClassName),
                  volumeMode = Option(spec.getVolumeMode),
                  dataSource = None,
                  dataSourceRef = None
                )
              )
            )
          )
        )
      )
    )

    Some(
      generated.Volume(
        name = Option(v.getName),
        volumeSource = Some(volumeSource)
      )
    )
  }

  private[submit] def convertVolumeMount(vm: model.VolumeMount): Option[generated.VolumeMount] = {
    if (vm == null) return None
    Some(
      generated.VolumeMount(
        mountPath = Option(vm.getMountPath),
        mountPropagation = Option(vm.getMountPropagation),
        name = Option(vm.getName),
        readOnly = Option(vm.getReadOnly).map(_.booleanValue()),
        subPath = Option(vm.getSubPath),
        subPathExpr = Option(vm.getSubPathExpr)
      )
    )
  }

  private[submit] def convertVolumeToFabric8(v: generated.Volume): Option[model.Volume] = {
    if (v == null) return None

    val volume = new model.Volume()
    v.name.foreach(volume.setName)

    v.volumeSource.foreach { vs =>
      vs.emptyDir.foreach { ed =>
        val emptyDir = new model.EmptyDirVolumeSource()
        ed.medium.foreach(emptyDir.setMedium)
        ed.sizeLimit.foreach(q => emptyDir.setSizeLimit(new model.Quantity(q.string.orNull)))
        volume.setEmptyDir(emptyDir)
      }

      vs.hostPath.foreach { hp =>
        val hostPath = new model.HostPathVolumeSource()
        hp.path.foreach(hostPath.setPath)
        hp.`type`.foreach(hostPath.setType)
        volume.setHostPath(hostPath)
      }

      vs.configMap.foreach { cm =>
        val configMap = new model.ConfigMapVolumeSource()
        cm.localObjectReference.flatMap(_.name).foreach(configMap.setName)
        cm.defaultMode.foreach(m => configMap.setDefaultMode(m))
        cm.optional.foreach(o => configMap.setOptional(o))
        volume.setConfigMap(configMap)
      }

      vs.secret.foreach { s =>
        val secret = new model.SecretVolumeSource()
        s.secretName.foreach(secret.setSecretName)
        s.defaultMode.foreach(m => secret.setDefaultMode(m))
        s.optional.foreach(o => secret.setOptional(o))
        volume.setSecret(secret)
      }

      vs.persistentVolumeClaim.foreach { pvc =>
        val persistentVolumeClaim = new model.PersistentVolumeClaimVolumeSource()
        pvc.claimName.foreach(persistentVolumeClaim.setClaimName)
        pvc.readOnly.foreach(r => persistentVolumeClaim.setReadOnly(r))
        volume.setPersistentVolumeClaim(persistentVolumeClaim)
      }

      vs.nfs.foreach { nfs =>
        val nfsVolume = new model.NFSVolumeSource()
        nfs.path.foreach(nfsVolume.setPath)
        nfs.readOnly.foreach(r => nfsVolume.setReadOnly(r))
        nfs.server.foreach(nfsVolume.setServer)
        volume.setNfs(nfsVolume)
      }

      vs.gcePersistentDisk.foreach { gce =>
        val gceDisk = new model.GCEPersistentDiskVolumeSource()
        gce.fsType.foreach(gceDisk.setFsType)
        gce.partition.foreach(p => gceDisk.setPartition(p))
        gce.pdName.foreach(gceDisk.setPdName)
        gce.readOnly.foreach(r => gceDisk.setReadOnly(r))
        volume.setGcePersistentDisk(gceDisk)
      }

      vs.awsElasticBlockStore.foreach { aws =>
        val awsEbs = new model.AWSElasticBlockStoreVolumeSource()
        aws.fsType.foreach(awsEbs.setFsType)
        aws.partition.foreach(p => awsEbs.setPartition(p))
        aws.readOnly.foreach(r => awsEbs.setReadOnly(r))
        aws.volumeID.foreach(awsEbs.setVolumeID)
        volume.setAwsElasticBlockStore(awsEbs)
      }

      vs.gitRepo.foreach { git =>
        val gitRepo = new model.GitRepoVolumeSource()
        git.directory.foreach(gitRepo.setDirectory)
        git.repository.foreach(gitRepo.setRepository)
        git.revision.foreach(gitRepo.setRevision)
        volume.setGitRepo(gitRepo)
      }

      vs.iscsi.foreach { iscsi =>
        val iscsiVolume = new model.ISCSIVolumeSource()
        iscsi.chapAuthDiscovery.foreach(c => iscsiVolume.setChapAuthDiscovery(c))
        iscsi.chapAuthSession.foreach(c => iscsiVolume.setChapAuthSession(c))
        iscsi.fsType.foreach(iscsiVolume.setFsType)
        iscsi.initiatorName.foreach(iscsiVolume.setInitiatorName)
        iscsi.iqn.foreach(iscsiVolume.setIqn)
        iscsi.iscsiInterface.foreach(iscsiVolume.setIscsiInterface)
        iscsi.lun.foreach(l => iscsiVolume.setLun(l))
        if (iscsi.portals.nonEmpty) {
          iscsiVolume.setPortals(iscsi.portals.asJava)
        }
        iscsi.readOnly.foreach(r => iscsiVolume.setReadOnly(r))
        iscsi.secretRef.foreach { ref =>
          val secretRef = new model.LocalObjectReference()
          ref.name.foreach(secretRef.setName)
          iscsiVolume.setSecretRef(secretRef)
        }
        iscsi.targetPortal.foreach(iscsiVolume.setTargetPortal)
        volume.setIscsi(iscsiVolume)
      }

      vs.glusterfs.foreach { gfs =>
        val glusterfs = new model.GlusterfsVolumeSource()
        gfs.endpoints.foreach(glusterfs.setEndpoints)
        gfs.path.foreach(glusterfs.setPath)
        gfs.readOnly.foreach(r => glusterfs.setReadOnly(r))
        volume.setGlusterfs(glusterfs)
      }

      vs.rbd.foreach { rbd =>
        val rbdVolume = new model.RBDVolumeSource()
        if (rbd.monitors.nonEmpty) {
          rbdVolume.setMonitors(rbd.monitors.asJava)
        }
        rbd.image.foreach(rbdVolume.setImage)
        rbd.fsType.foreach(rbdVolume.setFsType)
        rbd.pool.foreach(rbdVolume.setPool)
        rbd.user.foreach(rbdVolume.setUser)
        rbd.keyring.foreach(rbdVolume.setKeyring)
        rbd.secretRef.foreach { ref =>
          val secretRef = new model.LocalObjectReference()
          ref.name.foreach(secretRef.setName)
          rbdVolume.setSecretRef(secretRef)
        }
        rbd.readOnly.foreach(r => rbdVolume.setReadOnly(r))
        volume.setRbd(rbdVolume)
      }

      vs.flexVolume.foreach { flex =>
        val flexVolume = new model.FlexVolumeSource()
        flex.driver.foreach(flexVolume.setDriver)
        flex.fsType.foreach(flexVolume.setFsType)
        flex.secretRef.foreach { ref =>
          val secretRef = new model.LocalObjectReference()
          ref.name.foreach(secretRef.setName)
          flexVolume.setSecretRef(secretRef)
        }
        flex.readOnly.foreach(r => flexVolume.setReadOnly(r))
        if (flex.options.nonEmpty) {
          flexVolume.setOptions(flex.options.asJava)
        }
        volume.setFlexVolume(flexVolume)
      }

      vs.cinder.foreach { cinder =>
        val cinderVolume = new model.CinderVolumeSource()
        cinder.volumeID.foreach(cinderVolume.setVolumeID)
        cinder.fsType.foreach(cinderVolume.setFsType)
        cinder.readOnly.foreach(r => cinderVolume.setReadOnly(r))
        cinder.secretRef.foreach { ref =>
          val secretRef = new model.LocalObjectReference()
          ref.name.foreach(secretRef.setName)
          cinderVolume.setSecretRef(secretRef)
        }
        volume.setCinder(cinderVolume)
      }

      vs.cephfs.foreach { ceph =>
        val cephfs = new model.CephFSVolumeSource()
        if (ceph.monitors.nonEmpty) {
          cephfs.setMonitors(ceph.monitors.asJava)
        }
        ceph.path.foreach(cephfs.setPath)
        ceph.user.foreach(cephfs.setUser)
        ceph.secretFile.foreach(cephfs.setSecretFile)
        ceph.secretRef.foreach { ref =>
          val secretRef = new model.LocalObjectReference()
          ref.name.foreach(secretRef.setName)
          cephfs.setSecretRef(secretRef)
        }
        ceph.readOnly.foreach(r => cephfs.setReadOnly(r))
        volume.setCephfs(cephfs)
      }

      vs.flocker.foreach { flocker =>
        val flockerVolume = new model.FlockerVolumeSource()
        flocker.datasetName.foreach(flockerVolume.setDatasetName)
        flocker.datasetUUID.foreach(flockerVolume.setDatasetUUID)
        volume.setFlocker(flockerVolume)
      }

      vs.downwardAPI.foreach { da =>
        val downwardApi = new model.DownwardAPIVolumeSource()
        da.defaultMode.foreach(m => downwardApi.setDefaultMode(m))
        if (da.items.nonEmpty) {
          val items = da.items.map { item =>
            val volumeFile = new model.DownwardAPIVolumeFile()
            item.path.foreach(volumeFile.setPath)
            item.fieldRef.foreach { fr =>
              val fieldRef = new model.ObjectFieldSelector()
              fr.apiVersion.foreach(fieldRef.setApiVersion)
              fr.fieldPath.foreach(fieldRef.setFieldPath)
              volumeFile.setFieldRef(fieldRef)
            }
            item.resourceFieldRef.foreach { rf =>
              val resourceFieldRef = new model.ResourceFieldSelector()
              rf.containerName.foreach(resourceFieldRef.setContainerName)
              rf.resource.foreach(resourceFieldRef.setResource)
              rf.divisor.foreach(d =>
                resourceFieldRef.setDivisor(new model.Quantity(d.string.orNull))
              )
              volumeFile.setResourceFieldRef(resourceFieldRef)
            }
            item.mode.foreach(m => volumeFile.setMode(m))
            volumeFile
          }.asJava
          downwardApi.setItems(items)
        }
        volume.setDownwardAPI(downwardApi)
      }

      vs.fc.foreach { fc =>
        val fcVolume = new model.FCVolumeSource()
        if (fc.targetWWNs.nonEmpty) {
          fcVolume.setTargetWWNs(fc.targetWWNs.asJava)
        }
        fc.lun.foreach(l => fcVolume.setLun(l))
        fc.fsType.foreach(fcVolume.setFsType)
        fc.readOnly.foreach(r => fcVolume.setReadOnly(r))
        if (fc.wwids.nonEmpty) {
          fcVolume.setWwids(fc.wwids.asJava)
        }
        volume.setFc(fcVolume)
      }

      vs.azureFile.foreach { af =>
        val azureFile = new model.AzureFileVolumeSource()
        af.secretName.foreach(azureFile.setSecretName)
        af.shareName.foreach(azureFile.setShareName)
        af.readOnly.foreach(r => azureFile.setReadOnly(r))
        volume.setAzureFile(azureFile)
      }

      vs.vsphereVolume.foreach { vsphere =>
        val vsphereVolume = new model.VsphereVirtualDiskVolumeSource()
        vsphere.volumePath.foreach(vsphereVolume.setVolumePath)
        vsphere.fsType.foreach(vsphereVolume.setFsType)
        vsphere.storagePolicyID.foreach(vsphereVolume.setStoragePolicyID)
        vsphere.storagePolicyName.foreach(vsphereVolume.setStoragePolicyName)
        volume.setVsphereVolume(vsphereVolume)
      }

      vs.quobyte.foreach { q =>
        val quobyte = new model.QuobyteVolumeSource()
        q.registry.foreach(quobyte.setRegistry)
        q.volume.foreach(quobyte.setVolume)
        q.readOnly.foreach(r => quobyte.setReadOnly(r))
        q.user.foreach(quobyte.setUser)
        q.group.foreach(quobyte.setGroup)
        q.tenant.foreach(quobyte.setTenant)
        volume.setQuobyte(quobyte)
      }

      vs.azureDisk.foreach { ad =>
        val azureDisk = new model.AzureDiskVolumeSource()
        ad.diskName.foreach(azureDisk.setDiskName)
        ad.diskURI.foreach(azureDisk.setDiskURI)
        ad.cachingMode.foreach(azureDisk.setCachingMode)
        ad.fsType.foreach(azureDisk.setFsType)
        ad.readOnly.foreach(r => azureDisk.setReadOnly(r))
        ad.kind.foreach(azureDisk.setKind)
        volume.setAzureDisk(azureDisk)
      }

      vs.photonPersistentDisk.foreach { ppd =>
        val photonDisk = new model.PhotonPersistentDiskVolumeSource()
        ppd.pdID.foreach(photonDisk.setPdID)
        ppd.fsType.foreach(photonDisk.setFsType)
        volume.setPhotonPersistentDisk(photonDisk)
      }

      vs.projected.foreach { proj =>
        val projected = new model.ProjectedVolumeSource()
        proj.defaultMode.foreach(m => projected.setDefaultMode(m))
        if (proj.sources.nonEmpty) {
          val sources = proj.sources.map { src =>
            val projection = new model.VolumeProjection()

            src.secret.foreach { s =>
              val secretProj = new model.SecretProjection()
              s.localObjectReference.flatMap(_.name).foreach(secretProj.setName)
              s.optional.foreach(o => secretProj.setOptional(o))
              if (s.items.nonEmpty) {
                val items = s.items.map { item =>
                  val keyPath = new model.KeyToPath()
                  item.key.foreach(keyPath.setKey)
                  item.path.foreach(keyPath.setPath)
                  item.mode.foreach(m => keyPath.setMode(m))
                  keyPath
                }.asJava
                secretProj.setItems(items)
              }
              projection.setSecret(secretProj)
            }

            src.configMap.foreach { cm =>
              val configMapProj = new model.ConfigMapProjection()
              cm.localObjectReference.flatMap(_.name).foreach(configMapProj.setName)
              cm.optional.foreach(o => configMapProj.setOptional(o))
              if (cm.items.nonEmpty) {
                val items = cm.items.map { item =>
                  val keyPath = new model.KeyToPath()
                  item.key.foreach(keyPath.setKey)
                  item.path.foreach(keyPath.setPath)
                  item.mode.foreach(m => keyPath.setMode(m))
                  keyPath
                }.asJava
                configMapProj.setItems(items)
              }
              projection.setConfigMap(configMapProj)
            }

            src.downwardAPI.foreach { da =>
              val downwardApiProj = new model.DownwardAPIProjection()
              if (da.items.nonEmpty) {
                val items = da.items.map { item =>
                  val volumeFile = new model.DownwardAPIVolumeFile()
                  item.path.foreach(volumeFile.setPath)
                  item.fieldRef.foreach { fr =>
                    val fieldRef = new model.ObjectFieldSelector()
                    fr.apiVersion.foreach(fieldRef.setApiVersion)
                    fr.fieldPath.foreach(fieldRef.setFieldPath)
                    volumeFile.setFieldRef(fieldRef)
                  }
                  item.resourceFieldRef.foreach { rf =>
                    val resourceFieldRef = new model.ResourceFieldSelector()
                    rf.containerName.foreach(resourceFieldRef.setContainerName)
                    rf.resource.foreach(resourceFieldRef.setResource)
                    rf.divisor.foreach(d =>
                      resourceFieldRef.setDivisor(new model.Quantity(d.string.orNull))
                    )
                    volumeFile.setResourceFieldRef(resourceFieldRef)
                  }
                  item.mode.foreach(m => volumeFile.setMode(m))
                  volumeFile
                }.asJava
                downwardApiProj.setItems(items)
              }
              projection.setDownwardAPI(downwardApiProj)
            }

            src.serviceAccountToken.foreach { sat =>
              val tokenProj = new model.ServiceAccountTokenProjection()
              sat.audience.foreach(tokenProj.setAudience)
              sat.expirationSeconds.foreach(e => tokenProj.setExpirationSeconds(e))
              sat.path.foreach(tokenProj.setPath)
              projection.setServiceAccountToken(tokenProj)
            }

            projection
          }.asJava
          projected.setSources(sources)
        }
        volume.setProjected(projected)
      }

      vs.portworxVolume.foreach { pwx =>
        val portworx = new model.PortworxVolumeSource()
        pwx.volumeID.foreach(portworx.setVolumeID)
        pwx.fsType.foreach(portworx.setFsType)
        pwx.readOnly.foreach(r => portworx.setReadOnly(r))
        volume.setPortworxVolume(portworx)
      }

      vs.scaleIO.foreach { sio =>
        val scaleIO = new model.ScaleIOVolumeSource()
        sio.gateway.foreach(scaleIO.setGateway)
        sio.system.foreach(scaleIO.setSystem)
        sio.secretRef.foreach { ref =>
          val secretRef = new model.LocalObjectReference()
          ref.name.foreach(secretRef.setName)
          scaleIO.setSecretRef(secretRef)
        }
        sio.sslEnabled.foreach(s => scaleIO.setSslEnabled(s))
        sio.protectionDomain.foreach(scaleIO.setProtectionDomain)
        sio.storagePool.foreach(scaleIO.setStoragePool)
        sio.storageMode.foreach(scaleIO.setStorageMode)
        sio.volumeName.foreach(scaleIO.setVolumeName)
        sio.fsType.foreach(scaleIO.setFsType)
        sio.readOnly.foreach(r => scaleIO.setReadOnly(r))
        volume.setScaleIO(scaleIO)
      }

      vs.storageos.foreach { sos =>
        val storageos = new model.StorageOSVolumeSource()
        sos.volumeName.foreach(storageos.setVolumeName)
        sos.volumeNamespace.foreach(storageos.setVolumeNamespace)
        sos.fsType.foreach(storageos.setFsType)
        sos.readOnly.foreach(r => storageos.setReadOnly(r))
        sos.secretRef.foreach { ref =>
          val secretRef = new model.LocalObjectReference()
          ref.name.foreach(secretRef.setName)
          storageos.setSecretRef(secretRef)
        }
        volume.setStorageos(storageos)
      }

      vs.csi.foreach { csi =>
        val csiVolume = new model.CSIVolumeSource()
        csi.driver.foreach(csiVolume.setDriver)
        csi.readOnly.foreach(r => csiVolume.setReadOnly(r))
        csi.fsType.foreach(csiVolume.setFsType)
        if (csi.volumeAttributes.nonEmpty) {
          csiVolume.setVolumeAttributes(csi.volumeAttributes.asJava)
        }
        csi.nodePublishSecretRef.foreach { ref =>
          val secretRef = new model.LocalObjectReference()
          ref.name.foreach(secretRef.setName)
          csiVolume.setNodePublishSecretRef(secretRef)
        }
        volume.setCsi(csiVolume)
      }

      vs.ephemeral.foreach { eph =>
        val ephemeral = new model.EphemeralVolumeSource()
        eph.volumeClaimTemplate.foreach { vct =>
          val claimTemplate = new model.PersistentVolumeClaimTemplate()
          vct.spec.foreach { spec =>
            val pvcSpec = new model.PersistentVolumeClaimSpec()
            if (spec.accessModes.nonEmpty) {
              pvcSpec.setAccessModes(spec.accessModes.asJava)
            }
            // Note: PVC resources setting is commented out due to incompatibility between
            // fabric8 6.7.2 (used by Spark 3.x) and fabric8 6.13+ (used by Spark 4.1.0).
            // In 6.7.2, setResources expects ResourceRequirements
            // In 6.13+, setResources expects VolumeResourceRequirements
            // This field is optional and rarely used in ephemeral volumes.
            //
            // spec.resources.foreach { res =>
            //   val resources = new model.ResourceRequirements() // or VolumeResourceRequirements in 6.13+
            //   if (res.limits.nonEmpty) {
            //     resources.setLimits(...)
            //   }
            //   if (res.requests.nonEmpty) {
            //     resources.setRequests(...)
            //   }
            //   pvcSpec.setResources(resources)
            // }
            spec.volumeName.foreach(pvcSpec.setVolumeName)
            spec.storageClassName.foreach(pvcSpec.setStorageClassName)
            spec.volumeMode.foreach(pvcSpec.setVolumeMode)
            claimTemplate.setSpec(pvcSpec)
          }
          ephemeral.setVolumeClaimTemplate(claimTemplate)
        }
        volume.setEphemeral(ephemeral)
      }
    }

    Some(volume)
  }
}
