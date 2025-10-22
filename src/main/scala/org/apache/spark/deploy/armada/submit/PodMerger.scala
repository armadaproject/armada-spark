package org.apache.spark.deploy.armada.submit

import io.fabric8.kubernetes.api.model.{Pod, PodBuilder}
import k8s.io.api.core.v1.generated.{PodSpec => ProtobufPodSpec}
import scala.jdk.CollectionConverters._

/** Clean, simple Pod merging using fabric8 builder pattern.
  *
  * Key insight: PodBuilder(pod) preserves ALL fields automatically. We only override the 3 arrays
  * that need strategic merge by name. Everything else (40+ fields) is handled generically by
  * fabric8.
  *
  * This solves the duplicate volume/initContainer bug via mergeByName.
  */
private[submit] object PodMerger {

  /** Strategic merge of two fabric8 Pods.
    *
    * Arrays (volumes, containers, initContainers) merge by name - overriding wins for duplicates.
    * All other fields automatically preserved from overriding Pod via PodBuilder.
    *
    * This is truly generic - works for ANY Pod fields without enumeration!
    */
  def mergePods(base: Pod, overriding: Pod): Pod = {
    val baseSpec = Option(base.getSpec).getOrElse(new io.fabric8.kubernetes.api.model.PodSpec())
    val overridingSpec =
      Option(overriding.getSpec).getOrElse(new io.fabric8.kubernetes.api.model.PodSpec())

    val mergedVolumes = mergeByName(
      Option(baseSpec.getVolumes).map(_.asScala.toSeq).getOrElse(Seq.empty),
      Option(overridingSpec.getVolumes).map(_.asScala.toSeq).getOrElse(Seq.empty)
    )(v => Option(v.getName))

    val mergedInitContainers = mergeByName(
      Option(baseSpec.getInitContainers).map(_.asScala.toSeq).getOrElse(Seq.empty),
      Option(overridingSpec.getInitContainers).map(_.asScala.toSeq).getOrElse(Seq.empty)
    )(c => Option(c.getName))

    val mergedContainers = mergeByName(
      Option(baseSpec.getContainers).map(_.asScala.toSeq).getOrElse(Seq.empty),
      Option(overridingSpec.getContainers).map(_.asScala.toSeq).getOrElse(Seq.empty)
    )(c => Option(c.getName))

    val baseNodeSelector =
      Option(baseSpec.getNodeSelector).map(_.asScala.toMap).getOrElse(Map.empty)
    val overridingNodeSelector =
      Option(overridingSpec.getNodeSelector).map(_.asScala.toMap).getOrElse(Map.empty)
    val mergedNodeSelector = baseNodeSelector ++ overridingNodeSelector

    new PodBuilder(overriding)
      .editOrNewSpec()
      .withVolumes(mergedVolumes.asJava)
      .withInitContainers(mergedInitContainers.asJava)
      .withContainers(mergedContainers.asJava)
      .withNodeSelector(if (mergedNodeSelector.nonEmpty) mergedNodeSelector.asJava else null)
      .endSpec()
      .build()
  }

  /** Merge arrays by name - simple strategic merge. Items from 'overriding' replace items from
    * 'base' with same name.
    */
  def mergeByName[T](base: Seq[T], overriding: Seq[T])(
      getName: T => Option[String]
  ): Seq[T] = {
    val overridingNames = overriding.flatMap(getName(_).toSeq).toSet
    val uniqueBase      = base.filterNot(item => getName(item).exists(overridingNames.contains))
    uniqueBase ++ overriding
  }

  /** Convert protobuf PodSpec to fabric8 Pod for merging.
    */
  def protobufToFabric8Pod(protobufPodSpec: ProtobufPodSpec): Pod = {
    val fabric8Spec = PodSpecConverter.protobufToFabric8(protobufPodSpec)
    new PodBuilder()
      .withNewMetadata()
      .endMetadata()
      .withSpec(fabric8Spec)
      .build()
  }

  /** Convert fabric8 Pod back to protobuf PodSpec.
    */
  def fabric8PodToProtobuf(pod: Pod): ProtobufPodSpec = {
    PodSpecConverter.fabric8ToProtobuf(pod.getSpec)
  }
}
