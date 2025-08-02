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

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{
  ExternalClusterManager,
  SchedulerBackend,
  TaskScheduler,
  TaskSchedulerImpl
}
import org.apache.spark.util.ThreadUtils

private[spark] class ArmadaClusterManager extends ExternalClusterManager with Logging {
  val master   = "armada"
  val protocol = s"local://$master://"

  override def canCreate(masterURL: String): Boolean = {
    masterURL.toLowerCase.startsWith(protocol)
  }

  private def isLocal(conf: SparkConf): Boolean =
    true

  override def createTaskScheduler(sc: SparkContext, masterURL: String): TaskScheduler = {
    val maxTaskFailures = 1
    new TaskSchedulerImpl(sc, maxTaskFailures, isLocal(sc.conf))
  }

  override def createSchedulerBackend(
      sc: SparkContext,
      masterURL: String,
      scheduler: TaskScheduler
  ): SchedulerBackend = {
    val schedulerExecutorService =
      ThreadUtils.newDaemonSingleThreadScheduledExecutor("kubernetes-executor-maintenance")

    new ArmadaClusterManagerBackend(
      scheduler.asInstanceOf[TaskSchedulerImpl],
      sc,
      schedulerExecutorService,
      masterURL
    )
  }

  override def initialize(scheduler: TaskScheduler, backend: SchedulerBackend): Unit = {
    scheduler.asInstanceOf[TaskSchedulerImpl].initialize(backend)
  }
}
