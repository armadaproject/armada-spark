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

import org.apache.spark.deploy.armada.Config.{
  ARMADA_EXECUTOR_CONNECTION_TIMEOUT,
  ARMADA_DRIVER_LIMIT_CORES,
  ARMADA_DRIVER_LIMIT_MEMORY,
  ARMADA_DRIVER_REQUEST_CORES,
  ARMADA_DRIVER_REQUEST_MEMORY,
  ARMADA_EXECUTOR_LIMIT_CORES,
  ARMADA_EXECUTOR_LIMIT_MEMORY,
  ARMADA_EXECUTOR_REQUEST_CORES,
  ARMADA_EXECUTOR_REQUEST_MEMORY,
  ARMADA_HEALTH_CHECK_TIMEOUT,
  ARMADA_JOB_GANG_SCHEDULING_NODE_UNIFORMITY,
  ARMADA_JOB_NODE_SELECTORS,
  ARMADA_JOB_QUEUE,
  ARMADA_JOB_SET_ID,
  ARMADA_LOOKOUTURL,
  ARMADA_SERVER_INTERNAL_URL,
  ARMADA_SPARK_DRIVER_LABELS,
  ARMADA_SPARK_EXECUTOR_LABELS,
  ARMADA_SPARK_JOB_NAMESPACE,
  ARMADA_SPARK_JOB_PRIORITY,
  ARMADA_SPARK_POD_LABELS,
  DEFAULT_SPARK_EXECUTOR_CORES,
  DEFAULT_SPARK_EXECUTOR_MEMORY,
  CONTAINER_IMAGE,
  commaSeparatedLabelsToMap
}

import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration._
import _root_.io.armadaproject.armada.ArmadaClient
import k8s.io.api.core.v1.generated._
import k8s.io.apimachinery.pkg.api.resource.generated.Quantity
import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkApplication
import org.apache.spark.scheduler.cluster.SchedulerBackendUtils

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
    val armadaJobConfig = validateArmadaJobConfig(sparkConf)

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
    val (driverJobId, _) =
      submitArmadaJob(armadaClient, clientArguments, armadaJobConfig, sparkConf)

    val lookoutBaseURL = sparkConf.get(ARMADA_LOOKOUTURL)
    val lookoutURL =
      s"$lookoutBaseURL/?page=0&sort[id]=jobId&sort[desc]=true&" +
        s"ps=50&sb=$driverJobId&active=false&refresh=true"
    log(s"Lookout URL for the driver job is $lookoutURL")

    ()
  }

  private[spark] def validateArmadaJobConfig(conf: SparkConf): ArmadaJobConfig = {
    val queue = conf.get(ARMADA_JOB_QUEUE).getOrElse {
      throw new IllegalArgumentException(
        s"Queue name must be set via ${ARMADA_JOB_QUEUE.key}"
      )
    }

    val nodeSelectors       = conf.get(ARMADA_JOB_NODE_SELECTORS).map(commaSeparatedLabelsToMap)
    val gangUniformityLabel = conf.get(ARMADA_JOB_GANG_SCHEDULING_NODE_UNIFORMITY)

    if (nodeSelectors.isEmpty && gangUniformityLabel.isEmpty) {
      throw new IllegalArgumentException(
        s"Either ${ARMADA_JOB_NODE_SELECTORS.key} or ${ARMADA_JOB_GANG_SCHEDULING_NODE_UNIFORMITY.key} must be set."
      )
    }

    val jobSetId = conf.get(ARMADA_JOB_SET_ID) match {
      case Some(id) if id.nonEmpty => id
      case Some(_) =>
        throw new IllegalArgumentException(
          s"Empty jobSetId is not allowed. " +
            s"Please set a valid jobSetId via ${ARMADA_JOB_SET_ID.key}"
        )
      case None => conf.getAppId
    }

    val containerImage = conf.get(CONTAINER_IMAGE) match {
      case Some(image) if image.nonEmpty => image
      case Some(_) =>
        throw new IllegalArgumentException(
          s"Empty container image is not allowed. " +
            s"Please set a valid container image via ${CONTAINER_IMAGE.key}"
        )
      case None =>
        throw new IllegalArgumentException(
          s"Container image must be set via ${CONTAINER_IMAGE.key}"
        )
    }

    val podLabels =
      conf.get(ARMADA_SPARK_POD_LABELS).map(commaSeparatedLabelsToMap).getOrElse(Map.empty)
    val driverLabels =
      conf.get(ARMADA_SPARK_DRIVER_LABELS).map(commaSeparatedLabelsToMap).getOrElse(Map.empty)
    val executorLabels =
      conf.get(ARMADA_SPARK_EXECUTOR_LABELS).map(commaSeparatedLabelsToMap).getOrElse(Map.empty)

    val armadaClientUrl = conf.get("spark.master")
    val armadaClusterUrl =
      if (conf.get(ARMADA_SERVER_INTERNAL_URL).nonEmpty)
        s"local://armada://${conf.get(ARMADA_SERVER_INTERNAL_URL)}"
      else {
        armadaClientUrl
      }

    ArmadaJobConfig(
      queue = queue,
      jobSetId = jobSetId,
      namespace = conf.get(ARMADA_SPARK_JOB_NAMESPACE),
      priority = conf.get(ARMADA_SPARK_JOB_PRIORITY),
      containerImage = containerImage,
      podLabels = podLabels,
      driverLabels = driverLabels,
      executorLabels = executorLabels,
      nodeSelectors = nodeSelectors.getOrElse(Map.empty),
      nodeUniformityLabel = gangUniformityLabel,
      armadaClusterUrl = armadaClusterUrl,
      executorConnectionTimeout = Duration(conf.get(ARMADA_EXECUTOR_CONNECTION_TIMEOUT), SECONDS)
    )
  }

  private[spark] case class ArmadaJobConfig(
      queue: String,
      jobSetId: String,
      namespace: String,
      priority: Double,
      containerImage: String,
      podLabels: Map[String, String],
      driverLabels: Map[String, String],
      executorLabels: Map[String, String],
      armadaClusterUrl: String,
      nodeSelectors: Map[String, String],
      nodeUniformityLabel: Option[String],
      executorConnectionTimeout: Duration
  )

  private def submitArmadaJob(
      armadaClient: ArmadaClient,
      clientArguments: ClientArguments,
      armadaJobConfig: ArmadaJobConfig,
      conf: SparkConf
  ): (String, Seq[String]) = {
    val primaryResource = clientArguments.mainAppResource match {
      case JavaMainAppResource(Some(resource)) => Seq(resource)
      case PythonMainAppResource(resource)     => Seq(resource)
      case RMainAppResource(resource)          => Seq(resource)
      case _                                   => Seq()
    }

    val executorCount = SchedulerBackendUtils.getInitialTargetExecutorNumber(conf)

    val configGenerator = new ConfigGenerator("armada-spark-config", conf)
    val annotations = configGenerator.getAnnotations ++ armadaJobConfig.nodeUniformityLabel
      .map(label =>
        GangSchedulingAnnotations(
          None,
          1 + executorCount,
          label
        )
      )
      .getOrElse(Map.empty)
    val globalLabels = armadaJobConfig.podLabels
    val driverLabels =
      globalLabels ++ armadaJobConfig.driverLabels
    val nodeSelectors = armadaJobConfig.nodeSelectors

    val driverPort = 7078

    val confSeq = conf.getAll.flatMap { case (k, v) =>
      Seq("--conf", s"$k=$v")
    }
    val driver = newSparkDriverJobSubmitRequestItem(
      armadaJobConfig.armadaClusterUrl,
      armadaJobConfig.namespace,
      armadaJobConfig.priority,
      annotations,
      driverLabels,
      armadaJobConfig.containerImage,
      driverPort,
      clientArguments.mainClass,
      configGenerator.getVolumes,
      configGenerator.getVolumeMounts,
      nodeSelectors,
      confSeq ++ primaryResource ++ clientArguments.driverArgs,
      conf
    )

    val driverResponse =
      armadaClient.submitJobs(armadaJobConfig.queue, armadaJobConfig.jobSetId, Seq(driver))
    val driverJobId = driverResponse.jobResponseItems.head.jobId
    log(
      s"Submitted driver job with ID: $driverJobId, Error: ${driverResponse.jobResponseItems.head.error}"
    )

    val executorLabels =
      globalLabels ++ armadaJobConfig.executorLabels
    val driverHostname = ArmadaUtils.buildServiceNameFromJobId(driverJobId)
    val executors = (0 until executorCount).map { index =>
      newExecutorJobSubmitItem(
        index,
        armadaJobConfig.priority,
        armadaJobConfig.namespace,
        annotations,
        executorLabels,
        armadaJobConfig.containerImage,
        javaOptEnvVars(conf),
        armadaJobConfig.nodeUniformityLabel,
        driverHostname,
        driverPort,
        configGenerator.getVolumes,
        nodeSelectors,
        armadaJobConfig.executorConnectionTimeout,
        conf
      )
    }

    val executorsResponse =
      armadaClient.submitJobs(armadaJobConfig.queue, armadaJobConfig.jobSetId, executors)
    val executorJobIds = executorsResponse.jobResponseItems.map(item => {
      log(
        s"Submitted executor job with ID: ${item.jobId}, Error: ${item.error}"
      )
      item.jobId
    })

    (driverJobId, executorJobIds)
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

  private def newSparkDriverJobSubmitRequestItem(
      master: String,
      namespace: String,
      priority: Double,
      annotations: Map[String, String],
      labels: Map[String, String],
      driverImage: String,
      driverPort: Int,
      mainClass: String,
      volumes: Seq[Volume],
      volumeMounts: Seq[VolumeMount],
      nodeSelectors: Map[String, String],
      additionalDriverArgs: Seq[String],
      conf: SparkConf
  ): api.submit.JobSubmitRequestItem = {
    val container = newSparkDriverContainer(
      master,
      driverImage,
      driverPort,
      mainClass,
      volumeMounts,
      additionalDriverArgs,
      conf
    )
    val podSpec = PodSpec()
      .withTerminationGracePeriodSeconds(0)
      .withRestartPolicy("Never")
      .withContainers(Seq(container))
      .withVolumes(volumes)
      .withNodeSelector(nodeSelectors)

    api.submit
      .JobSubmitRequestItem()
      .withPriority(priority)
      .withNamespace(namespace)
      .withLabels(labels)
      .withAnnotations(annotations)
      .withPodSpec(podSpec)
      .withServices(
        Seq(
          api.submit.ServiceConfig(
            api.submit.ServiceType.Headless,
            Seq(driverPort)
          )
        )
      )
  }

  private def newSparkDriverContainer(
      master: String,
      image: String,
      port: Int,
      mainClass: String,
      volumeMounts: Seq[VolumeMount],
      additionalDriverArgs: Seq[String],
      conf: SparkConf
  ): Container = {
    val source = EnvVarSource().withFieldRef(
      ObjectFieldSelector()
        .withApiVersion("v1")
        .withFieldPath("status.podIP")
    )
    val envVars = Seq(
      EnvVar().withName("SPARK_DRIVER_BIND_ADDRESS").withValueFrom(source),
      EnvVar()
        .withName(ConfigGenerator.ENV_SPARK_CONF_DIR)
        .withValue(ConfigGenerator.REMOTE_CONF_DIR_NAME)
    )

    val driverLimits = Map(
      "memory" -> Quantity(Option(conf.get(ARMADA_DRIVER_LIMIT_MEMORY))),
      "cpu"    -> Quantity(Option(conf.get(ARMADA_DRIVER_LIMIT_CORES)))
    )
    val driverRequests = Map(
      "memory" -> Quantity(Option(conf.get(ARMADA_DRIVER_REQUEST_MEMORY))),
      "cpu"    -> Quantity(Option(conf.get(ARMADA_DRIVER_REQUEST_CORES)))
    )
    Container()
      .withName("driver")
      .withImagePullPolicy("IfNotPresent")
      .withImage(image)
      .withEnv(envVars)
      .withCommand(Seq("/opt/entrypoint.sh"))
      .withVolumeMounts(volumeMounts)
      .withPorts(
        Seq(
          ContainerPort(name = Option("driver"), containerPort = Option(port))
        )
      )
      .withArgs(
        Seq(
          "driver",
          "--verbose",
          "--master",
          master,
          "--class",
          mainClass,
          "--conf",
          s"spark.driver.port=$port",
          "--conf",
          "spark.driver.host=$(SPARK_DRIVER_BIND_ADDRESS)"
        ) ++ additionalDriverArgs
      )
      .withResources( // FIXME: What are reasonable requests/limits for spark drivers?
        ResourceRequirements(
          requests = driverRequests,
          limits = driverLimits
        )
      )
  }

  private def newExecutorJobSubmitItem(
      index: Int,
      priority: Double,
      namespace: String,
      annotations: Map[String, String],
      labels: Map[String, String],
      image: String,
      javaOptEnvVars: Seq[EnvVar],
      nodeUniformityLabel: Option[String],
      driverHostname: String,
      driverPort: Int,
      volumes: Seq[Volume],
      nodeSelectors: Map[String, String],
      connectionTimeout: Duration,
      conf: SparkConf
  ): api.submit.JobSubmitRequestItem = {
    println(s"driver service name $driverHostname")
    val initContainer = newExecutorInitContainer(driverHostname, driverPort, connectionTimeout)
    val container = newExecutorContainer(
      index,
      image,
      driverHostname,
      driverPort,
      nodeUniformityLabel,
      javaOptEnvVars,
      conf
    )
    val podSpec = PodSpec()
      .withTerminationGracePeriodSeconds(0)
      .withRestartPolicy("Never")
      .withInitContainers(Seq(initContainer))
      .withContainers(Seq(container))
      .withVolumes(volumes)
      .withNodeSelector(nodeSelectors)
    api.submit
      .JobSubmitRequestItem()
      .withPriority(priority)
      .withNamespace(namespace)
      .withLabels(labels)
      .withAnnotations(annotations)
      .withPodSpec(podSpec)
  }

  private def newExecutorInitContainer(
      driverHost: String,
      driverPort: Int,
      connectionTimeout: Duration
  ) = {
    Container()
      .withName("init")
      .withImagePullPolicy("IfNotPresent")
      .withImage("busybox")
      .withEnv(
        Seq(
          EnvVar().withName("SPARK_DRIVER_HOST").withValue(driverHost),
          EnvVar().withName("SPARK_DRIVER_PORT").withValue(driverPort.toString),
          EnvVar()
            .withName("SPARK_EXECUTOR_CONNECTION_TIMEOUT")
            .withValue(connectionTimeout.toSeconds.toString)
        )
      )
      .withCommand(Seq("sh", "-c"))
      .withArgs(
        Seq(ArmadaUtils.initContainerCommand)
      )
  }

  private def newExecutorContainer(
      index: Int,
      image: String,
      driverHostname: String,
      driverPort: Int,
      nodeUniformityLabel: Option[String],
      javaOptEnvVars: Seq[EnvVar],
      conf: SparkConf
  ): Container = {
    val driverURL = s"spark://CoarseGrainedScheduler@$driverHostname:$driverPort"
    val source = EnvVarSource().withFieldRef(
      ObjectFieldSelector()
        .withApiVersion("v1")
        .withFieldPath("status.podIP")
    )
    val DEFAULT_ARMADA_APP_ID = "armada-spark-app-id"
    val podName = EnvVarSource().withFieldRef(
      ObjectFieldSelector()
        .withApiVersion("v1")
        .withFieldPath("metadata.name")
    )
    val sparkExecutorMemory =
      conf.getOption("spark.executor.memory").getOrElse(DEFAULT_SPARK_EXECUTOR_MEMORY)
    val sparkExecutorCores =
      conf.getOption("spark.executor.cores").getOrElse(DEFAULT_SPARK_EXECUTOR_CORES)

    val executorLimits = Map(
      "memory" -> Quantity(Option(conf.get(ARMADA_EXECUTOR_LIMIT_MEMORY))),
      "cpu"    -> Quantity(Option(conf.get(ARMADA_EXECUTOR_LIMIT_CORES)))
    )
    val executorRequests = Map(
      "memory" -> Quantity(Option(conf.get(ARMADA_EXECUTOR_REQUEST_MEMORY))),
      "cpu"    -> Quantity(Option(conf.get(ARMADA_EXECUTOR_REQUEST_CORES)))
    )
    val envVars = Seq(
      EnvVar().withName("SPARK_EXECUTOR_ID").withValue(index.toString),
      EnvVar().withName("SPARK_RESOURCE_PROFILE_ID").withValue("0"),
      EnvVar().withName("SPARK_EXECUTOR_POD_NAME").withValueFrom(podName),
      EnvVar()
        .withName("SPARK_APPLICATION_ID")
        .withValue(conf.getOption("spark.app.id").getOrElse(DEFAULT_ARMADA_APP_ID)),
      EnvVar().withName("SPARK_EXECUTOR_CORES").withValue(sparkExecutorCores),
      EnvVar().withName("SPARK_EXECUTOR_MEMORY").withValue(sparkExecutorMemory),
      EnvVar().withName("SPARK_DRIVER_URL").withValue(driverURL),
      EnvVar().withName("SPARK_EXECUTOR_POD_IP").withValueFrom(source)
    ) ++ nodeUniformityLabel
      .map(label => EnvVar().withName("ARMADA_SPARK_GANG_NODE_UNIFORMITY_LABEL").withValue(label))
    Container()
      .withName("executor")
      .withImagePullPolicy("IfNotPresent")
      .withImage(image)
      .withEnv(envVars ++ javaOptEnvVars)
      .withCommand(Seq("/opt/entrypoint.sh"))
      .withArgs(
        Seq(
          "executor"
        )
      )
      .withResources(
        ResourceRequirements(
          requests = executorRequests,
          limits = executorLimits
        )
      )
  }
}
