package org.apache.spark.scheduler.armada

import org.apache.spark.SparkContext
import org.apache.spark.executor.ExecutorMetrics
import org.apache.spark.scheduler.SchedulingMode.SchedulingMode
import org.apache.spark.scheduler._
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

private[spark] class ArmadaClusterManager extends ExternalClusterManager {
  val master = "armada"
  val protocol = s"local://$master://"

  override def canCreate(masterURL: String): Boolean = {
    println(s"Connecting to Armada Control Plane: $masterURL")
    masterURL.toLowerCase.startsWith(protocol)
  }

  override def createTaskScheduler(sc: SparkContext, masterURL: String): TaskScheduler = {
    new TaskScheduler {

      override def applicationAttemptId(): Option[String] = {
        Some(s"$master-attempt-id")
      }

      override def schedulingMode: SchedulingMode = SchedulingMode.FIFO

      override def defaultParallelism(): Int = ???

      override def submitTasks(taskSet: TaskSet): Unit = ???


      override def executorLost(executorId: String, reason: ExecutorLossReason): Unit = ???

      override def rootPool: Pool = ???

      override def setDAGScheduler(dagScheduler: DAGScheduler): Unit = {
        println(s"[armada] dagScheduler: $dagScheduler")
      }

      override def start(): Unit = {
        println("[armada] start")
      }

      override def stop(exitCode: Int): Unit = ???

      override def cancelTasks(stageId: Int, interruptThread: Boolean, reason: String): Unit = ???

      override def killTaskAttempt(taskId: Long, interruptThread: Boolean, reason: String): Boolean = ???

      override def killAllTaskAttempts(stageId: Int, interruptThread: Boolean, reason: String): Unit = ???

      override def notifyPartitionCompletion(stageId: Int, partitionId: Int): Unit = ???

      override def executorHeartbeatReceived(execId: String, accumUpdates: Array[(Long, Seq[AccumulatorV2[_, _]])], blockManagerId: BlockManagerId, executorUpdates: mutable.Map[(Int, Int), ExecutorMetrics]): Boolean = ???

      override def executorDecommission(executorId: String, decommissionInfo: ExecutorDecommissionInfo): Unit = ???

      override def getExecutorDecommissionState(executorId: String): Option[ExecutorDecommissionState] = ???

      override def workerRemoved(workerId: String, host: String, message: String): Unit = ???
    }
  }
  override def createSchedulerBackend(sc: SparkContext, masterURL: String, scheduler: TaskScheduler): SchedulerBackend = null

  override def initialize(scheduler: TaskScheduler, backend: SchedulerBackend): Unit =
    println("ArmadaClusterManager initialized")
}
