package org.apache.spark.deploy.armada.submit

import java.util.UUID

private[spark] object GangSchedulingAnnotations {
  // See armadaproject/armada/docs/scheduling.md for an explanation of these labels.
  val GANG_ID = "armadaproject.io/gangId"
  val GANG_CARDINALITY = "armadaproject.io/gangCardinality"
  val GANG_NODE_UNIFORMITY_LABEL = "armadaproject.io/gangNodeUniformityLabel"

  def GetGangAnnotations(id: String, cardinality: Int, label: String): Map[String,String] = {
    var idStr = id
    if (idStr == "") {
      idStr = UUID.randomUUID.toString
    }

    Map(
        GangSchedulingAnnotations.GANG_ID -> idStr,
        GangSchedulingAnnotations.GANG_CARDINALITY -> cardinality.toString,
        GangSchedulingAnnotations.GANG_NODE_UNIFORMITY_LABEL -> label
    )
  }
}
