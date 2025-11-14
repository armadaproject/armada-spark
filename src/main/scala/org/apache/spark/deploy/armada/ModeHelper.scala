package org.apache.spark.deploy.armada

import org.apache.spark.SparkConf
import org.apache.spark.scheduler.cluster.SchedulerBackendUtils

trait ModeHelper {
  def getExecutorCount: Int
  def getGangCardinality: Int
}

class StaticCluster(conf: SparkConf) extends ModeHelper {
  override def getExecutorCount: Int = {
    SchedulerBackendUtils.getInitialTargetExecutorNumber(conf)
  }
  override def getGangCardinality: Int = {
    getExecutorCount + 1
  }
}

class StaticClient(conf: SparkConf) extends ModeHelper {
  override def getExecutorCount: Int = {
    SchedulerBackendUtils.getInitialTargetExecutorNumber(conf)
  }
  override def getGangCardinality: Int = {
    getExecutorCount
  }
}

class DynamicCluster(conf: SparkConf) extends ModeHelper {
  override def getExecutorCount: Int = {
    conf.getInt(
      "spark.dynamicAllocation.minExecutors",
      SchedulerBackendUtils.getInitialTargetExecutorNumber(conf)
    )
  }
  override def getGangCardinality: Int = {
    getExecutorCount + 1
  }
}

class DynamicClient(conf: SparkConf) extends ModeHelper {
  override def getExecutorCount: Int = {
    conf.getInt(
      "spark.dynamicAllocation.minExecutors",
      SchedulerBackendUtils.getInitialTargetExecutorNumber(conf)
    )
  }
  override def getGangCardinality: Int = {
    getExecutorCount
  }
}

object ModeHelper {
  def apply(conf: SparkConf): ModeHelper = {
    val deployMode = conf.get("spark.submit.deployMode", "client")
    val isDynamic  = conf.getBoolean("spark.dynamicAllocation.enabled", false)

    (deployMode.toLowerCase, isDynamic) match {
      case ("cluster", true)  => new DynamicCluster(conf)
      case ("cluster", false) => new StaticCluster(conf)
      case ("client", true)   => new DynamicClient(conf)
      case _                  => new StaticClient(conf)
    }
  }
}
