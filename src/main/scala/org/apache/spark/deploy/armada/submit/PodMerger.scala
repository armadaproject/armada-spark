package org.apache.spark.deploy.armada.submit

import io.fabric8.kubernetes.api.model.Pod
import io.fabric8.kubernetes.client.utils.Serialization
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}
import k8s.io.api.core.v1.generated.PodSpec
import scala.jdk.CollectionConverters._

/** Pod merging using JSON deep merge with Kubernetes strategic merge semantics.
  *
  * Preserves base fields when overriding doesn't set them, ensuring serviceAccount, tolerations,
  * etc. from feature steps are kept when templates don't specify them.
  */
private[submit] object PodMerger {

  private val mapper = new ObjectMapper()

  /** Strategic merge of protobuf PodSpecs: optimized to minimize conversions. */
  def mergePodSpecs(base: PodSpec, overriding: PodSpec): PodSpec = {
    val basePod       = PodSpecConverter.protobufPodSpecToFabric8Pod(base)
    val overridingPod = PodSpecConverter.protobufPodSpecToFabric8Pod(overriding)
    val mergedPod     = mergePods(basePod, overridingPod)
    PodSpecConverter.fabric8PodToProtobufPodSpec(mergedPod)
  }

  /** Strategic merge of fabric8 Pods (for compatibility with existing fabric8-based volume
    * merging).
    */
  def mergePods(base: Pod, overriding: Pod): Pod = {
    val merged = deepMerge(
      mapper.readTree(Serialization.asJson(base)).asInstanceOf[ObjectNode],
      mapper.readTree(Serialization.asJson(overriding)).asInstanceOf[ObjectNode],
      mapper
    )
    Serialization.unmarshal(merged.toString, classOf[Pod])
  }

  /** Deep merge JSON nodes: objects merge recursively, strategic arrays merge by name, null
    * preserves base.
    */
  private def deepMerge(
      base: JsonNode,
      overriding: JsonNode,
      mapper: ObjectMapper,
      path: String = ""
  ): JsonNode = {
    if (overriding == null || overriding.isNull) {
      return base
    }

    if (base == null || base.isNull) {
      return overriding
    }

    if (overriding.isObject && base.isObject) {
      val baseObj       = base.asInstanceOf[ObjectNode]
      val overridingObj = overriding.asInstanceOf[ObjectNode]
      val result        = mapper.createObjectNode()

      val baseFields = baseObj.fieldNames()
      while (baseFields.hasNext) {
        val fieldName = baseFields.next()
        result.set(fieldName, baseObj.get(fieldName))
      }

      val overridingFields = overridingObj.fieldNames()
      while (overridingFields.hasNext) {
        val fieldName       = overridingFields.next()
        val overridingValue = overridingObj.get(fieldName)
        val baseValue       = baseObj.get(fieldName)

        val newPath = if (path.isEmpty) fieldName else s"$path.$fieldName"
        result.set(fieldName, deepMerge(baseValue, overridingValue, mapper, newPath))
      }

      result
    } else if (overriding.isArray && base.isArray) {
      path match {
        case "spec.volumes" | "spec.containers" | "spec.initContainers" =>
          strategicMergeArray(
            base.asInstanceOf[ArrayNode],
            overriding.asInstanceOf[ArrayNode],
            "name",
            mapper
          )
        case _ =>
          overriding
      }
    } else {
      overriding
    }
  }

  /** Merge arrays by key field - overriding items replace base items with same key. */
  private def strategicMergeArray(
      base: ArrayNode,
      overriding: ArrayNode,
      keyField: String,
      mapper: ObjectMapper
  ): ArrayNode = {
    val result = mapper.createArrayNode()

    val overridingKeys = overriding
      .elements()
      .asScala
      .filter(item => item.isObject && item.has(keyField))
      .map(_.get(keyField).asText())
      .toSet

    // Add base items that don't conflict
    base.elements().asScala.foreach { item =>
      val shouldAdd = if (item.isObject && item.has(keyField)) {
        !overridingKeys.contains(item.get(keyField).asText())
      } else true

      if (shouldAdd) result.add(item)
    }

    // Add all overriding items
    overriding.elements().asScala.foreach(result.add)

    result
  }

  /** Merge arrays by name - overriding items replace base items with same name. */
  def mergeByName[T](base: Seq[T], overriding: Seq[T])(
      getName: T => Option[String]
  ): Seq[T] = {
    val baseMap       = base.flatMap(item => getName(item).map(_ -> item)).toMap
    val overridingMap = overriding.flatMap(item => getName(item).map(_ -> item)).toMap
    (baseMap ++ overridingMap).values.toSeq
  }
}
