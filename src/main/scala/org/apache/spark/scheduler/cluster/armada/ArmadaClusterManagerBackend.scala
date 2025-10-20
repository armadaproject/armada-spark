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
package org.apache.spark.scheduler.cluster.armada

import org.apache.spark.SparkContext
import org.apache.spark.deploy.armada.Config.{ARMADA_EXECUTOR_TRACKER_POLLING_INTERVAL, ARMADA_EXECUTOR_TRACKER_TIMEOUT}
import org.apache.spark.deploy.armada.submit.{ArmadaClientApplication, ArmadaUtils}
import org.apache.spark.rpc.{RpcAddress, RpcCallContext}
import org.apache.spark.scheduler.cluster.{CoarseGrainedSchedulerBackend, SchedulerBackendUtils}
import org.apache.spark.scheduler.{ExecutorDecommission, TaskSchedulerImpl}
import org.apache.spark.util.{Clock, SystemClock, ThreadUtils}

import java.util.concurrent.{ScheduledExecutorService, TimeUnit}
import scala.collection.mutable

private[spark] class ArmadaClusterManagerBackend(
    scheduler: TaskSchedulerImpl,
    sc: SparkContext,
    executorService: ScheduledExecutorService,
    masterURL: String
) extends CoarseGrainedSchedulerBackend(scheduler, sc.env.rpcEnv) {

  private val initialExecutors = SchedulerBackendUtils.getInitialTargetExecutorNumber(conf)
  private val executorTracker  = new ExecutorTracker(new SystemClock(), initialExecutors)

  override def applicationId(): String = {
    conf.getAppId
  }

  override def start(): Unit = {
    // NOTE: armada-spark driver submits executors alongside driver.
    // No need to start them here.
    logInfo("gbj1: Armada Cluster Backend: starting")
    new ArmadaClientApplication().start(new Array[String](0), conf)
    executorTracker.start()
  }

  // Track executors to make sure we have the expected number
  class ExecutorTracker(val clock: Clock, val numberOfExecutors: Int) {

    private val daemon =
      ThreadUtils.newDaemonSingleThreadScheduledExecutor("armada-min-executor-daemon")
    private val pollingInterval = conf.get(ARMADA_EXECUTOR_TRACKER_POLLING_INTERVAL)
    private val timeout         = conf.get(ARMADA_EXECUTOR_TRACKER_TIMEOUT)

    private var startTime = 0L

    def start(): Unit = {
      daemon.scheduleWithFixedDelay(
        () => checkMin(),
        pollingInterval,
        pollingInterval,
        TimeUnit.MILLISECONDS
      )
    }

    def checkMin(): Unit = {
      logInfo("Checking number of Executors.  Should be: " + numberOfExecutors)
      val count = getAliveCount
      if (count < numberOfExecutors) {
        logInfo("Found " + count + " Executors running")
        if (startTime == 0) {
          startTime = clock.getTimeMillis()
        } else if (clock.getTimeMillis() - startTime > timeout) {
          scheduler.error("Insufficient executors running.  Driver exiting.")
        }
      } else {
        startTime = 0
      }

    }

    private def getAliveCount: Int = {
      ArmadaUtils
        .getExecutorRange(numberOfExecutors)
        .map(i => scheduler.isExecutorAlive(i.toString))
        .count(x => x)
    }

    def stop(): Unit = {
      daemon.shutdownNow()
    }
  }

  override def stop(): Unit = {
    executorTracker.stop()
  }

  /*
    override def doRequestTotalExecutors(
      resourceProfileToTotalExecs: Map[ResourceProfile, Int]): Future[Boolean] = {
        //podAllocator.setTotalExpectedExecutors(resourceProfileToTotalExecs)
        //Future.successful(true)
    }
   */

  override def sufficientResourcesRegistered(): Boolean = {
    totalRegisteredExecutors.get() >= initialExecutors * minRegisteredRatio
  }

  override def createDriverEndpoint(): DriverEndpoint = {
    new ArmadaDriverEndpoint()
  }

  private class ArmadaDriverEndpoint extends DriverEndpoint {
    private val execIDRequester = mutable.HashMap[RpcAddress, String]()

    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] =
      super.receiveAndReply(context)
    /* generateExecID(context).orElse(
          ignoreRegisterExecutorAtStoppedContext.orElse(
            super.receiveAndReply(context))) */

    override def onDisconnected(rpcAddress: RpcAddress): Unit = {
      val execId = addressToExecutorId.get(rpcAddress)
      execId match {
        case Some(id) =>
          executorsPendingDecommission.get(id) match {
            case Some(_) =>
              // We don't pass through the host because by convention the
              // host is only populated if the entire host is going away
              // and we don't know if that's the case or just one container.
              removeExecutor(id, ExecutorDecommission(None))
            case _ =>
              // Don't do anything besides disabling the executor - allow the K8s API events to
              // drive the rest of the lifecycle decisions.
              // If it's disconnected due to network issues eventually heartbeat will clear it up.
              disableExecutor(id)
          }
        case _ =>
          val newExecId = execIDRequester.get(rpcAddress)
          newExecId match {
            case Some(_) =>
              execIDRequester -= rpcAddress
            // Expected, executors re-establish a connection with an ID
            case _ =>
              logDebug(s"No executor found for $rpcAddress")
          }
      }
    }
  }
}
