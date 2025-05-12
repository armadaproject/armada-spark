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

import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration._

import _root_.io.armadaproject.armada.ArmadaClient
import k8s.io.api.core.v1.generated._
import k8s.io.apimachinery.pkg.api.resource.generated.Quantity
import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkApplication
import org.apache.spark.deploy.armada.Config.{
  ARMADA_CLUSTER_SELECTORS,
  ARMADA_HEALTH_CHECK_TIMEOUT,
  ARMADA_LOOKOUTURL,
  DEFAULT_CLUSTER_SELECTORS,
  DRIVER_SERVICE_NAME_PREFIX,
  commaSeparatedLabelsToMap,
  GANG_SCHEDULING_NODE_UNIFORMITY_LABEL,
  ARMADA_SPARK_GLOBAL_LABELS,
  ARMADA_SPARK_DRIVER_LABELS,
  ARMADA_SPARK_EXECUTOR_LABELS
}
import org.apache.spark.deploy.armada.submit.GangSchedulingAnnotations._
import org.apache.spark.scheduler.cluster.SchedulerBackendUtils

import java.util.UUID

/** Encapsulates arguments to the submission client.
  *
  * @param mainAppResource
  *   the main application resource if any
  * @param mainClass
  *   the main class of the application to run
  * @param driverArgs
  *   arguments to the driver
  */
private[spark] case class ClientArguments(
    mainAppResource: MainAppResource,
    mainClass: String,
    driverArgs: Array[String],
    proxyUser: Option[String]
)

private[spark] object ClientArguments {

  def fromCommandLineArgs(args: Array[String]): ClientArguments = {
    var mainAppResource: MainAppResource = JavaMainAppResource(None)
    var mainClass: Option[String]        = None
    val driverArgs                       = mutable.ArrayBuffer.empty[String]
    var proxyUser: Option[String]        = None

    args.sliding(2, 2).toList.foreach {
      case Array("--primary-java-resource", primaryJavaResource: String) =>
        mainAppResource = JavaMainAppResource(Some(primaryJavaResource))
      case Array("--primary-py-file", primaryPythonResource: String) =>
        mainAppResource = PythonMainAppResource(primaryPythonResource)
      case Array("--primary-r-file", primaryRFile: String) =>
        mainAppResource = RMainAppResource(primaryRFile)
      case Array("--main-class", clazz: String) =>
        mainClass = Some(clazz)
      case Array("--arg", arg: String) =>
        driverArgs += arg
      case Array("--proxy-user", user: String) =>
        proxyUser = Some(user)
      case other =>
        val invalid = other.mkString(" ")
        throw new RuntimeException(s"Unknown arguments: $invalid")
    }

    require(
      mainClass.isDefined,
      "Main class must be specified via --main-class"
    )

    ClientArguments(
      mainAppResource,
      mainClass.get,
      driverArgs.toArray,
      proxyUser
    )
  }
}

private[spark] object Client {
  def submissionId(namespace: String, driverPodName: String): String =
    s"$namespace:$driverPodName"
}

/** Main class and entry point of application submission in KUBERNETES mode.
  */
private[spark] class ArmadaClientApplication extends SparkApplication {
  // FIXME: Find the real way to log properly.
  private def log(msg: String): Unit = {
    // scalastyle:off println
    System.err.println(msg)
    // scalastyle:on println
  }

  override def start(args: Array[String], conf: SparkConf): Unit = {
    log("ArmadaClientApplication.start() called!")
    val parsedArguments = ClientArguments.fromCommandLineArgs(args)
    run(parsedArguments, conf)
  }

  private def run(
      clientArguments: ClientArguments,
      sparkConf: SparkConf
  ): Unit = {
    val (host, port) = ArmadaUtils.parseMasterUrl(sparkConf.get("spark.master"))
    log(s"host is $host, port is $port")
    val armadaClient = ArmadaClient(host, port)
    val healthTimeout =
      Duration(sparkConf.get(ARMADA_HEALTH_CHECK_TIMEOUT), SECONDS)
    val healthResp = Await.result(armadaClient.submitHealth(), healthTimeout)

    if (healthResp.status.isServing) {
      log("Submit health good!")
    } else {
      log("Could not contact Armada!")
    }

    // # FIXME: Need to check how this is launched whether to submit a job or
    // to turn into driver / cluster manager mode.
    val jobId = submitDriverJob(armadaClient, clientArguments, sparkConf)

    val lookoutBaseURL = sparkConf.get(ARMADA_LOOKOUTURL)
    val lookoutURL =
      s"$lookoutBaseURL/?page=0&sort[id]=jobId&sort[desc]=true&" +
        s"ps=50&sb=$jobId&active=false&refresh=true"
    log(s"Lookout URL for this job is $lookoutURL")

    ()
  }

  private def getExecutorContainers(
      numExecutors: Int,
      driverServiceName: String,
      conf: SparkConf
  ): Seq[Container] = {
    (0 until numExecutors).map {
      getExecutorContainer(_, driverServiceName, conf)
    }
  }

  // Convert the space-delimited "spark.executor.extraJavaOptions" into env vars that can be used by entrypoint.sh
  private def javaOptEnvVars(conf: SparkConf) = {
    // The executor's java opts are handled as env vars in the docker entrypoint.sh here:
    // https://github.com/apache/spark/blob/v3.5.3/resource-managers/kubernetes/docker/src/main/dockerfiles/spark/entrypoint.sh#L44-L46

    // entrypoint.sh then adds those to the jvm command line here:
    // https://github.com/apache/spark/blob/v3.5.3/resource-managers/kubernetes/docker/src/main/dockerfiles/spark/entrypoint.sh#L96
    // TODO: this configuration option appears to not be set... is that a problem?
    val javaOpts =
      conf
        .getOption("spark.executor.extraJavaOptions")
        .map(_.split(" ").toSeq)
        .getOrElse(
          Seq()
        ) :+ "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"

    javaOpts.zipWithIndex.map { case (value: String, index) =>
      EnvVar().withName("SPARK_JAVA_OPT_" + index).withValue(value)
    }
  }

  private def getExecutorContainer(
      executorID: Int,
      driverServiceName: String,
      conf: SparkConf
  ): Container = {
    val driverURL = s"spark://CoarseGrainedScheduler@$driverServiceName:7078"
    val source = EnvVarSource().withFieldRef(
      ObjectFieldSelector()
        .withApiVersion("v1")
        .withFieldPath("status.podIP")
    )
    val envVars = Seq(
      EnvVar().withName("SPARK_EXECUTOR_ID").withValue(executorID.toString),
      EnvVar().withName("SPARK_RESOURCE_PROFILE_ID").withValue("0"),
      EnvVar().withName("SPARK_EXECUTOR_POD_NAME").withValue("test-pod-name"),
      EnvVar().withName("SPARK_APPLICATION_ID").withValue("test_spark_app_id"),
      EnvVar().withName("SPARK_EXECUTOR_CORES").withValue("1"),
      EnvVar().withName("SPARK_EXECUTOR_MEMORY").withValue("512m"),
      EnvVar().withName("SPARK_DRIVER_URL").withValue(driverURL),
      EnvVar().withName("SPARK_EXECUTOR_POD_IP").withValueFrom(source),
      EnvVar()
        .withName("ARMADA_SPARK_GANG_NODE_UNIFORMITY_LABEL")
        .withValue(conf.get(GANG_SCHEDULING_NODE_UNIFORMITY_LABEL))
    )
    Container()
      .withName(s"spark-executor-$executorID")
      .withImagePullPolicy("IfNotPresent")
      .withImage(conf.get("spark.kubernetes.container.image"))
      .withEnv(envVars ++ javaOptEnvVars(conf))
      .withCommand(Seq("/opt/entrypoint.sh"))
      .withArgs(
        Seq(
          "executor"
        )
      )
      .withResources(
        ResourceRequirements(
          limits = Map(
            "memory"            -> Quantity(Option("512Mi")),
            "ephemeral-storage" -> Quantity(Option("512Mi")),
            "cpu"               -> Quantity(Option("100m"))
          ),
          requests = Map(
            "memory"            -> Quantity(Option("512Mi")),
            "ephemeral-storage" -> Quantity(Option("512Mi")),
            "cpu"               -> Quantity(Option("100m"))
          )
        )
      )
  }

  private def submitDriverJob(
      armadaClient: ArmadaClient,
      clientArguments: ClientArguments,
      conf: SparkConf
  ): String = {
    val driverServiceName =
      conf.get(DRIVER_SERVICE_NAME_PREFIX) + UUID.randomUUID.toString
    val source = EnvVarSource().withFieldRef(
      ObjectFieldSelector()
        .withApiVersion("v1")
        .withFieldPath("status.podIP")
    )
    val numExecutors =
      SchedulerBackendUtils.getInitialTargetExecutorNumber(conf)
    val envVars = Seq(
      EnvVar().withName("SPARK_DRIVER_BIND_ADDRESS").withValueFrom(source),
      EnvVar()
        .withName(ConfigGenerator.ENV_SPARK_CONF_DIR)
        .withValue(ConfigGenerator.REMOTE_CONF_DIR_NAME),
      EnvVar()
        .withName("EXTERNAL_CLUSTER_SUPPORT_ENABLED")
        .withValue("true"),
      EnvVar()
        .withName("ARMADA_SPARK_DRIVER_SERVICE_NAME")
        .withValue(driverServiceName)
    )

    val configGenerator =
      new ConfigGenerator("armada-spark-config", conf)
    val primaryResource = clientArguments.mainAppResource match {
      case JavaMainAppResource(Some(resource)) => Seq(resource)
      case PythonMainAppResource(resource)     => Seq(resource)
      case RMainAppResource(resource)          => Seq(resource)
      case _                                   => Seq()
    }

    val confSeq = conf.getAll.flatMap { case (k, v) =>
      Seq("--conf", s"$k=$v")
    }
    val sparkDriverPort = 7078
    val driverContainer = Container()
      .withName("spark-driver")
      .withImagePullPolicy("IfNotPresent")
      .withImage(conf.get("spark.kubernetes.container.image"))
      .withEnv(envVars)
      .withCommand(Seq("/opt/entrypoint.sh"))
      .withVolumeMounts(configGenerator.getVolumeMounts)
      .withPorts(
        Seq(
          ContainerPort(
            Option("as-driver-port"),
            Option(0),
            Option(sparkDriverPort)
          )
        )
      )
      .withArgs(
        Seq(
          "driver",
          "--verbose",
          "--class",
          clientArguments.mainClass,
          "--master",
          "local://armada://armada-server.armada.svc.cluster.local:50051",
          "--conf",
          s"spark.driver.port=$sparkDriverPort",
          "--conf",
          "spark.driver.host=$(SPARK_DRIVER_BIND_ADDRESS)"
        ) ++ confSeq ++ primaryResource ++ clientArguments.driverArgs
      )
      .withResources( // FIXME: What are reasonable requests/limits for spark drivers?
        ResourceRequirements(
          limits = Map(
            "memory"            -> Quantity(Option("450Mi")),
            "ephemeral-storage" -> Quantity(Option("512Mi")),
            "cpu"               -> Quantity(Option("200m"))
          ),
          requests = Map(
            "memory"            -> Quantity(Option("450Mi")),
            "ephemeral-storage" -> Quantity(Option("512Mi")),
            "cpu"               -> Quantity(Option("200m"))
          )
        )
      )

    val executorContainers =
      getExecutorContainers(numExecutors, driverServiceName, conf)

    val gangAnnotations = GangSchedulingAnnotations(
      None,
      1 + numExecutors,
      conf.get(GANG_SCHEDULING_NODE_UNIFORMITY_LABEL)
    )

    val globalLabels = commaSeparatedLabelsToMap(conf.get(ARMADA_SPARK_GLOBAL_LABELS))
    val executorLabels =
      globalLabels ++ commaSeparatedLabelsToMap(conf.get(ARMADA_SPARK_EXECUTOR_LABELS))
    val executorJobs =
      for (container <- executorContainers)
        yield api.submit
          .JobSubmitRequestItem()
          .withPriority(0)
          .withNamespace("default")
          .withLabels(executorLabels)
          .withPodSpec(
            PodSpec()
              .withTerminationGracePeriodSeconds(0)
              .withRestartPolicy("Never")
              .withContainers(Seq(container))
              .withVolumes(configGenerator.getVolumes)
              .withNodeSelector(
                commaSeparatedLabelsToMap(conf.get(ARMADA_CLUSTER_SELECTORS))
              )
          )
          .withAnnotations(configGenerator.getAnnotations ++ gangAnnotations)

    val podSpec = PodSpec()
      .withTerminationGracePeriodSeconds(0)
      .withRestartPolicy("Never")
      .withContainers(Seq(driverContainer))
      .withVolumes(configGenerator.getVolumes)
      .withNodeSelector(
        commaSeparatedLabelsToMap(conf.get(ARMADA_CLUSTER_SELECTORS))
      )

    val driverLabels =
      globalLabels ++ commaSeparatedLabelsToMap(conf.get(ARMADA_SPARK_DRIVER_LABELS))
    val driverJob = api.submit
      .JobSubmitRequestItem()
      .withPriority(0)
      .withNamespace("default")
      .withLabels(driverLabels)
      .withPodSpec(podSpec)
      .withAnnotations(configGenerator.getAnnotations ++ gangAnnotations)
      .withServices(
        Seq(
          api.submit.ServiceConfig(
            api.submit.ServiceType.NodePort,
            Seq(sparkDriverPort),
            driverServiceName
          )
        )
      )

    val executorJobsSubmitResponse =
      armadaClient.submitJobs("test", "executor", executorJobs)
    for (respItem <- executorJobsSubmitResponse.jobResponseItems) {
      val error = if (respItem.error == "") "None" else respItem.error
      log(s"Executor JobID: ${respItem.jobId}  Error: $error")
    }

    // FIXME: Plumb config for queue, job-set-id
    val jobSubmitResponse =
      armadaClient.submitJobs("test", "driver", Seq(driverJob))

    for (respItem <- jobSubmitResponse.jobResponseItems) {
      val error = if (respItem.error == "") "None" else respItem.error
      log(s"Driver JobID: ${respItem.jobId}  Error: $error")
    }
    jobSubmitResponse.jobResponseItems.head.jobId
  }
}
