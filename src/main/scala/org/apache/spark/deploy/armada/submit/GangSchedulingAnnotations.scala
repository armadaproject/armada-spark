package org.apache.spark.deploy.armada.submit

import java.util.UUID

private[spark] object GangSchedulingAnnotations {
  // See armadaproject/armada/docs/scheduling.md for an explanation of these labels.
  val GANG_ID = "armadaproject.io/gangId"
  val GANG_CARDINALITY = "armadaproject.io/gangCardinality"
  val GANG_NODE_UNIFORMITY_LABEL = "armadaproject.io/gangNodeUniformityLabel"

  def apply(id: Option[String], cardinality: Int, label: String): Map[String,String] = {
    val nonEmptyId = id.filter(_.nonEmpty).getOrElse(UUID.randomUUID.toString)
    Map(
        GangSchedulingAnnotations.GANG_ID -> nonEmptyId,
        GangSchedulingAnnotations.GANG_CARDINALITY -> cardinality.toString,
        GangSchedulingAnnotations.GANG_NODE_UNIFORMITY_LABEL -> label
    )
  }
}
