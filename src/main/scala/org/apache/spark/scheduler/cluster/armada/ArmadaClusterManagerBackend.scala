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

import io.armadaproject.armada.ArmadaClient
import k8s.io.api.core.v1.generated._
import k8s.io.apimachinery.pkg.api.resource.generated.Quantity
import org.apache.spark.SparkContext
import org.apache.spark.deploy.armada.Config.{ARMADA_EXECUTOR_TRACKER_POLLING_INTERVAL, ARMADA_EXECUTOR_TRACKER_TIMEOUT}
import org.apache.spark.rpc.{RpcAddress, RpcCallContext}
import org.apache.spark.scheduler.cluster.{CoarseGrainedSchedulerBackend, SchedulerBackendUtils}
import org.apache.spark.scheduler.{ExecutorDecommission, TaskSchedulerImpl}
import org.apache.spark.util.{Clock, SystemClock, ThreadUtils}

import java.util.concurrent.{ScheduledExecutorService, TimeUnit}
import scala.collection.mutable.HashMap



// TODO: Implement for Armada
private[spark] class ArmadaClusterSchedulerBackend(
    scheduler: TaskSchedulerImpl,
    sc: SparkContext,
    executorService: ScheduledExecutorService,
    masterURL: String)
  extends CoarseGrainedSchedulerBackend(scheduler, sc.env.rpcEnv) {

  // FIXME
  private val appId = "fake_app_id_FIXME"

  private val initialExecutors = SchedulerBackendUtils.getInitialTargetExecutorNumber(conf)
  private val executorTracker = new ExecutorTracker(new SystemClock(), initialExecutors)


  override def applicationId(): String = {
    conf.getOption("spark.app.id").getOrElse(appId)
  }


  private def submitJob(executorId: Int): Unit = {

    val header = "local://"
    val masterWithoutHeader = masterURL.substring(header.length)
    val urlArray = masterWithoutHeader.split(":")
    // Remove leading "/"'s
    val host = if (urlArray(1).startsWith("/")) urlArray(1).substring(2) else urlArray(1)
    val port = urlArray(2).toInt

    val driverAddr = sys.env("SPARK_DRIVER_BIND_ADDRESS")


    val driverURL = s"spark://CoarseGrainedScheduler@$driverAddr:7078"
    val source = EnvVarSource().withFieldRef(ObjectFieldSelector()
      .withApiVersion("v1").withFieldPath("status.podIP"))
    val envVars = Seq(
      EnvVar().withName("SPARK_EXECUTOR_ID").withValue(executorId.toString),
      EnvVar().withName("SPARK_RESOURCE_PROFILE_ID").withValue("0"),
      EnvVar().withName("SPARK_EXECUTOR_POD_NAME").withValue("test-pod-name"),
      EnvVar().withName("SPARK_APPLICATION_ID").withValue("test_spark_app_id"),
      EnvVar().withName("SPARK_EXECUTOR_CORES").withValue("1"),
      EnvVar().withName("SPARK_EXECUTOR_MEMORY").withValue("512m"),
      EnvVar().withName("SPARK_DRIVER_URL").withValue(driverURL),
      EnvVar().withName("SPARK_JAVA_OPT_1").withValue("--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"),
      EnvVar().withName("SPARK_EXECUTOR_POD_IP").withValueFrom(source)
    )
    val executorContainer = Container()
      .withName("spark-executor")
      .withImagePullPolicy("IfNotPresent")
      .withImage(conf.get("spark.kubernetes.container.image"))
      .withEnv(envVars)
      .withCommand(Seq("/opt/entrypoint.sh"))
      .withArgs(
        Seq(
          "executor"
        )
      )
      .withResources(
        ResourceRequirements(
          limits = Map(
            "memory" -> Quantity(Option("1000Mi")),
            "cpu" -> Quantity(Option("100m"))
          ),
          requests = Map(
            "memory" -> Quantity(Option("1000Mi")),
            "cpu" -> Quantity(Option("100m"))
          )
        )
      )

    val podSpec = PodSpec()
      .withTerminationGracePeriodSeconds(0)
      .withRestartPolicy("Never")
      .withContainers(Seq(executorContainer))

    val testJob = api.submit
      .JobSubmitRequestItem()
      .withPriority(0)
      .withNamespace("default")
      .withPodSpec(podSpec)

    val client = ArmadaClient(host, port)
    val jobSubmitResponse = client.submitJobs("test", "executor", Seq(testJob))

    logInfo("Driver Job Submit Response")
    for (respItem <- jobSubmitResponse.jobResponseItems) {
      logInfo(s"JobID: ${respItem.jobId}  Error: ${respItem.error} ")

    }
  }

  override def start(): Unit = {
    1 to initialExecutors foreach {j: Int => submitJob(j)}
    executorTracker.start()
  }

  // Track executors to make sure we have the expected number
  class ExecutorTracker(val clock: Clock,
                        val numberOfExecutors: Int) {

    private val daemon = ThreadUtils.newDaemonSingleThreadScheduledExecutor("armada-min-executor-daemon")
    private val pollingInterval = conf.get(ARMADA_EXECUTOR_TRACKER_POLLING_INTERVAL)
    private val timeout = conf.get(ARMADA_EXECUTOR_TRACKER_TIMEOUT)

    private var startTime = 0L

    def start(): Unit = {
      daemon.scheduleWithFixedDelay(
        () => checkMin(),
        pollingInterval, pollingInterval, TimeUnit.MILLISECONDS)
    }

    def checkMin(): Unit = {
      logInfo("Checking number of Executors")
      if (getAliveCount < numberOfExecutors) {
        if (startTime == 0) {
          startTime = clock.getTimeMillis()
        }
        else if (clock.getTimeMillis() - startTime > timeout ) {
          scheduler.error("Inufficient executors running.  Driver exiting.")
        }
      } else {
        startTime = 0
      }

    }

    private def getAliveCount: Int = {
      (1 to numberOfExecutors).map(i => scheduler.isExecutorAlive(i.toString)).count(x => x)
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
    private val execIDRequester = new HashMap[RpcAddress, String]

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
