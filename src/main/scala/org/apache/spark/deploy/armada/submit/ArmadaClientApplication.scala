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
  ARMADA_JOB_TEMPLATE,
  ARMADA_DRIVER_JOB_ITEM_TEMPLATE,
  ARMADA_EXECUTOR_JOB_ITEM_TEMPLATE,
  ARMADA_LOOKOUTURL,
  ARMADA_SERVER_INTERNAL_URL,
  ARMADA_SPARK_DRIVER_LABELS,
  ARMADA_SPARK_EXECUTOR_LABELS,
  ARMADA_SPARK_JOB_NAMESPACE,
  ARMADA_SPARK_JOB_PRIORITY,
  ARMADA_SPARK_POD_LABELS,
  DEFAULT_SPARK_EXECUTOR_CORES,
  DEFAULT_SPARK_EXECUTOR_MEMORY,
  DEFAULT_CORES,
  DEFAULT_MEM,
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

  private[spark] def parseCLIConfig(conf: SparkConf): CLIConfig = {
    // Only extract CLI values, don't apply defaults or template values here
    // Note: Don't filter empty strings here - validation should handle that later
    val queue          = conf.get(ARMADA_JOB_QUEUE)
    val jobSetId       = conf.get(ARMADA_JOB_SET_ID)
    val containerImage = conf.get(CONTAINER_IMAGE)

    val nodeSelectors       = conf.get(ARMADA_JOB_NODE_SELECTORS).map(commaSeparatedLabelsToMap)
    val gangUniformityLabel = conf.get(ARMADA_JOB_GANG_SCHEDULING_NODE_UNIFORMITY)

    val podLabels =
      conf.get(ARMADA_SPARK_POD_LABELS).map(commaSeparatedLabelsToMap).getOrElse(Map.empty)
    val driverLabels =
      conf.get(ARMADA_SPARK_DRIVER_LABELS).map(commaSeparatedLabelsToMap).getOrElse(Map.empty)
    val executorLabels =
      conf.get(ARMADA_SPARK_EXECUTOR_LABELS).map(commaSeparatedLabelsToMap).getOrElse(Map.empty)

    val armadaClientUrl = conf.get("spark.master")
    val armadaClusterUrl = conf
      .get(ARMADA_SERVER_INTERNAL_URL)
      .filter(_.nonEmpty)
      .map { internalUrl =>
        s"local://armada://$internalUrl"
      }
      .getOrElse(armadaClientUrl)

    // Parse resource limits and requests without defaults
    val driverLimitCores      = conf.get(ARMADA_DRIVER_LIMIT_CORES)
    val driverRequestCores    = conf.get(ARMADA_DRIVER_REQUEST_CORES)
    val driverLimitMemory     = conf.get(ARMADA_DRIVER_LIMIT_MEMORY)
    val driverRequestMemory   = conf.get(ARMADA_DRIVER_REQUEST_MEMORY)
    val executorLimitCores    = conf.get(ARMADA_EXECUTOR_LIMIT_CORES)
    val executorRequestCores  = conf.get(ARMADA_EXECUTOR_REQUEST_CORES)
    val executorLimitMemory   = conf.get(ARMADA_EXECUTOR_LIMIT_MEMORY)
    val executorRequestMemory = conf.get(ARMADA_EXECUTOR_REQUEST_MEMORY)

    CLIConfig(
      queue = queue,
      jobSetId = jobSetId,
      namespace = conf.get(ARMADA_SPARK_JOB_NAMESPACE),
      priority = Some(conf.get(ARMADA_SPARK_JOB_PRIORITY)),
      containerImage = containerImage,
      podLabels = podLabels,
      driverLabels = driverLabels,
      executorLabels = executorLabels,
      nodeSelectors = nodeSelectors.getOrElse(Map.empty),
      nodeUniformityLabel = gangUniformityLabel,
      armadaClusterUrl = Some(armadaClusterUrl),
      executorConnectionTimeout =
        Some(Duration(conf.get(ARMADA_EXECUTOR_CONNECTION_TIMEOUT), SECONDS)),
      driverLimitCores = driverLimitCores,
      driverRequestCores = driverRequestCores,
      driverLimitMemory = driverLimitMemory,
      driverRequestMemory = driverRequestMemory,
      executorLimitCores = executorLimitCores,
      executorRequestCores = executorRequestCores,
      executorLimitMemory = executorLimitMemory,
      executorRequestMemory = executorRequestMemory
    )
  }

  /** Resolves a value based on precedence: CLI > Template > Default
    *
    * @param cliValue
    *   Value from CLI configuration
    * @param templateValue
    *   Value from template
    * @param defaultValue
    *   Default value to use if neither CLI nor template provide a value
    * @return
    *   Resolved value based on precedence
    */
  private def resolveValue[T](
      cliValue: Option[T],
      templateValue: => Option[T],
      defaultValue: => T
  ): T = {
    cliValue.orElse(templateValue).getOrElse(defaultValue)
  }

  /** Resolves a required value based on precedence: CLI > Template > Error
    *
    * @param cliValue
    *   Value from CLI configuration
    * @param templateValue
    *   Value from template
    * @param errorMessage
    *   Error message if no value is found
    * @return
    *   Resolved value based on precedence
    */
  private def resolveRequiredValue[T](
      cliValue: Option[T],
      templateValue: => Option[T],
      errorMessage: => String
  ): T = {
    cliValue.orElse(templateValue).getOrElse(throw new IllegalArgumentException(errorMessage))
  }

  /** Helper class to resolve common configuration values using precedence */
  private case class ResolvedConfig(
      namespace: String,
      priority: Double,
      containerImage: String,
      armadaClusterUrl: String,
      executorConnectionTimeout: Duration
  )

  /** Resolves all required configuration values using precedence */
  private def resolveConfig(cliConfig: CLIConfig, conf: SparkConf): ResolvedConfig = {
    val namespace = resolveValue(cliConfig.namespace, None, "default")
    val priority  = resolveValue(cliConfig.priority, None, 0.0)
    val containerImage = resolveRequiredValue(
      cliConfig.containerImage.filter(_.nonEmpty),
      None,
      s"Container image must be set via ${CONTAINER_IMAGE.key}"
    )
    val armadaClusterUrl = resolveValue(
      cliConfig.armadaClusterUrl,
      None,
      conf.get("spark.master")
    )
    val executorConnectionTimeout = resolveValue(
      cliConfig.executorConnectionTimeout,
      None,
      Duration(conf.get(ARMADA_EXECUTOR_CONNECTION_TIMEOUT), SECONDS)
    )

    ResolvedConfig(namespace, priority, containerImage, armadaClusterUrl, executorConnectionTimeout)
  }

  private[spark] def validateArmadaJobConfig(conf: SparkConf): ArmadaJobConfig = {
    val jobTemplate = conf.get(ARMADA_JOB_TEMPLATE) match {
      case Some(path) =>
        try {
          Some(JobTemplateLoader.loadJobTemplate(path))
        } catch {
          case e: Exception =>
            throw new RuntimeException(
              s"Failed to load job template from: $path - ${e.getMessage}",
              e
            )
        }
      case None => None
    }

    val driverJobItemTemplate = conf.get(ARMADA_DRIVER_JOB_ITEM_TEMPLATE) match {
      case Some(path) =>
        try {
          Some(JobTemplateLoader.loadJobItemTemplate(path))
        } catch {
          case e: Exception =>
            throw new RuntimeException(
              s"Failed to load driver job item template from: $path - ${e.getMessage}",
              e
            )
        }
      case None => None
    }

    val executorJobItemTemplate = conf.get(ARMADA_EXECUTOR_JOB_ITEM_TEMPLATE) match {
      case Some(path) =>
        try {
          Some(JobTemplateLoader.loadJobItemTemplate(path))
        } catch {
          case e: Exception =>
            throw new RuntimeException(
              s"Failed to load executor job item template from: $path - ${e.getMessage}",
              e
            )
        }
      case None => None
    }

    val cliConfig = parseCLIConfig(conf)

    // Validate required configuration values
    validateRequiredConfig(cliConfig, jobTemplate, conf)

    ArmadaJobConfig(
      jobTemplate = jobTemplate,
      driverJobItemTemplate = driverJobItemTemplate,
      executorJobItemTemplate = executorJobItemTemplate,
      cliConfig = cliConfig
    )
  }

  /** Validates required configuration values and throws IllegalArgumentException if invalid.
    *
    * @param cliConfig
    *   CLI configuration parsed from --conf options
    * @param jobTemplate
    *   Optional job template that may provide fallback values
    * @param conf
    *   Spark configuration
    * @throws IllegalArgumentException
    *   if required values are missing or invalid
    */
  private def validateRequiredConfig(
      cliConfig: CLIConfig,
      jobTemplate: Option[api.submit.JobSubmitRequest],
      conf: SparkConf
  ): Unit = {
    // Validate queue - must not be empty string
    val queueFromCli      = cliConfig.queue.filter(_.nonEmpty)
    val queueFromTemplate = jobTemplate.map(_.queue).filter(_.nonEmpty)

    // Check for explicitly empty queue first
    if (cliConfig.queue.exists(_.isEmpty)) {
      throw new IllegalArgumentException(
        s"Queue name must be set via ${ARMADA_JOB_QUEUE.key} or in the job template."
      )
    }

    if (queueFromCli.isEmpty && queueFromTemplate.isEmpty) {
      throw new IllegalArgumentException(
        s"Queue name must be set via ${ARMADA_JOB_QUEUE.key} or in the job template."
      )
    }

    // Validate jobSetId - must not be empty string
    if (cliConfig.jobSetId.exists(_.isEmpty)) {
      throw new IllegalArgumentException(
        s"Empty jobSetId is not allowed. Please set a valid jobSetId via ${ARMADA_JOB_SET_ID.key}"
      )
    }

    // Validate container image - must be present and not empty
    // Check if it was set but empty first
    if (cliConfig.containerImage.exists(_.isEmpty)) {
      throw new IllegalArgumentException(
        s"Empty container image is not allowed. Please set a valid container image via ${CONTAINER_IMAGE.key}"
      )
    } else if (cliConfig.containerImage.isEmpty) {
      // Not set at all
      throw new IllegalArgumentException(
        s"Container image must be set via ${CONTAINER_IMAGE.key}"
      )
    }
  }

  /** Configuration object for CLI-parsed parameters from --conf options.
    *
    * @param queue
    *   Armada queue name for job submission
    * @param jobSetId
    *   Unique identifier for grouping related jobs
    * @param namespace
    *   Kubernetes namespace for job execution
    * @param priority
    *   Job priority for scheduling (higher values = higher priority)
    * @param containerImage
    *   Docker image to use for Spark containers
    * @param podLabels
    *   Kubernetes labels applied to all pods (driver and executors)
    * @param driverLabels
    *   Additional Kubernetes labels applied only to driver pod
    * @param executorLabels
    *   Additional Kubernetes labels applied only to executor pods
    * @param armadaClusterUrl
    *   URL for connecting to Armada cluster
    * @param nodeSelectors
    *   Kubernetes node selectors for pod placement
    * @param nodeUniformityLabel
    *   Label key for gang scheduling node uniformity constraints
    * @param executorConnectionTimeout
    *   Maximum time to wait for executor connection to driver
    * @param driverLimitCores
    *   Hard CPU limit for the driver pod
    * @param driverRequestCores
    *   CPU request for the driver pod
    * @param driverLimitMemory
    *   Hard memory limit for the driver pod
    * @param driverRequestMemory
    *   Memory request for the driver pod
    * @param executorLimitCores
    *   Hard CPU limit for each executor pod
    * @param executorRequestCores
    *   CPU request for each executor pod
    * @param executorLimitMemory
    *   Hard memory limit for each executor pod
    * @param executorRequestMemory
    *   Memory request for each executor pod
    */
  private[spark] case class CLIConfig(
      queue: Option[String],
      jobSetId: Option[String],
      namespace: Option[String],
      priority: Option[Double],
      containerImage: Option[String],
      podLabels: Map[String, String],
      driverLabels: Map[String, String],
      executorLabels: Map[String, String],
      armadaClusterUrl: Option[String],
      nodeSelectors: Map[String, String],
      nodeUniformityLabel: Option[String],
      executorConnectionTimeout: Option[Duration],
      driverLimitCores: Option[String],
      driverRequestCores: Option[String],
      driverLimitMemory: Option[String],
      driverRequestMemory: Option[String],
      executorLimitCores: Option[String],
      executorRequestCores: Option[String],
      executorLimitMemory: Option[String],
      executorRequestMemory: Option[String]
  )

  /** Configuration object for Armada job submission parameters.
    *
    * @param jobTemplate
    *   Optional loaded job template for advanced job customization
    * @param driverJobItemTemplate
    *   Optional loaded driver job item template
    * @param executorJobItemTemplate
    *   Optional loaded executor job item template
    * @param cliConfig
    *   CLI configuration parameters parsed from --conf options
    */
  private[spark] case class ArmadaJobConfig(
      jobTemplate: Option[api.submit.JobSubmitRequest],
      driverJobItemTemplate: Option[api.submit.JobSubmitRequestItem],
      executorJobItemTemplate: Option[api.submit.JobSubmitRequestItem],
      cliConfig: CLIConfig
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

    // Resolve values using precedence function
    val finalQueue = resolveRequiredValue(
      armadaJobConfig.cliConfig.queue.filter(_.nonEmpty),
      armadaJobConfig.jobTemplate.map(_.queue).filter(_.nonEmpty),
      s"Queue name must be set via ${ARMADA_JOB_QUEUE.key} or in the job template."
    )

    val finalJobSetId = armadaJobConfig.cliConfig.jobSetId match {
      case Some(id) if id.nonEmpty => id
      case Some(_) =>
        throw new IllegalArgumentException(
          s"Empty jobSetId is not allowed. Please set a valid jobSetId via ${ARMADA_JOB_SET_ID.key}"
        )
      case None =>
        armadaJobConfig.jobTemplate.map(_.jobSetId).filter(_.nonEmpty).getOrElse(conf.getAppId)
    }

    val (templateAnnotations, templateLabels) = armadaJobConfig.jobTemplate
      .map { templateRequest =>
        if (templateRequest.jobRequestItems.nonEmpty) {
          val firstItem = templateRequest.jobRequestItems.head
          (firstItem.annotations, firstItem.labels)
        } else {
          (Map.empty[String, String], Map.empty[String, String])
        }
      }
      .getOrElse((Map.empty[String, String], Map.empty[String, String]))

    // Resolve configuration values
    val resolvedConfig = resolveConfig(armadaJobConfig.cliConfig, conf)

    val configGenerator = new ConfigGenerator("armada-spark-config", conf)
    val annotations = configGenerator.getAnnotations ++
      templateAnnotations ++
      armadaJobConfig.cliConfig.nodeUniformityLabel
        .map(label =>
          GangSchedulingAnnotations(
            None,
            1 + executorCount,
            label
          )
        )
        .getOrElse(Map.empty)

    // Merge template labels with config labels, config takes precedence
    val globalLabels  = templateLabels ++ armadaJobConfig.cliConfig.podLabels
    val driverLabels  = globalLabels ++ armadaJobConfig.cliConfig.driverLabels
    val nodeSelectors = armadaJobConfig.cliConfig.nodeSelectors

    val driverPort = 7078

    val confSeq = conf.getAll.flatMap { case (k, v) =>
      Seq("--conf", s"$k=$v")
    }
    val driver = armadaJobConfig.driverJobItemTemplate match {
      case Some(template) =>
        // Use driver template and merge with runtime configuration
        mergeDriverTemplate(
          template,
          resolvedConfig,
          armadaJobConfig,
          annotations,
          driverLabels,
          driverPort,
          clientArguments.mainClass,
          configGenerator.getVolumes,
          configGenerator.getVolumeMounts,
          nodeSelectors,
          confSeq ++ primaryResource ++ clientArguments.driverArgs,
          conf
        )
      case None =>
        // Use default driver creation
        newSparkDriverJobSubmitRequestItem(
          resolvedConfig.armadaClusterUrl,
          resolvedConfig.namespace,
          resolvedConfig.priority,
          annotations,
          driverLabels,
          resolvedConfig.containerImage,
          driverPort,
          clientArguments.mainClass,
          configGenerator.getVolumes,
          configGenerator.getVolumeMounts,
          nodeSelectors,
          confSeq ++ primaryResource ++ clientArguments.driverArgs,
          armadaJobConfig
        )
    }

    val driverResponse =
      armadaClient.submitJobs(finalQueue, finalJobSetId, Seq(driver))
    val driverJobId = driverResponse.jobResponseItems.head.jobId
    log(
      s"Submitted driver job with ID: $driverJobId, Error: ${driverResponse.jobResponseItems.head.error}"
    )

    val executorLabels =
      globalLabels ++ armadaJobConfig.cliConfig.executorLabels
    val driverHostname = ArmadaUtils.buildServiceNameFromJobId(driverJobId)
    val executors = (0 until executorCount).map { index =>
      armadaJobConfig.executorJobItemTemplate match {
        case Some(template) =>
          // Use executor template and merge with runtime configuration
          mergeExecutorTemplate(
            template,
            index,
            armadaJobConfig,
            annotations,
            executorLabels,
            javaOptEnvVars(conf),
            driverHostname,
            driverPort,
            configGenerator.getVolumes,
            nodeSelectors,
            conf
          )
        case None =>
          // Use default executor creation
          newExecutorJobSubmitItem(
            index,
            resolvedConfig.priority,
            resolvedConfig.namespace,
            annotations,
            executorLabels,
            resolvedConfig.containerImage,
            javaOptEnvVars(conf),
            armadaJobConfig.cliConfig.nodeUniformityLabel,
            driverHostname,
            driverPort,
            configGenerator.getVolumes,
            nodeSelectors,
            resolvedConfig.executorConnectionTimeout,
            armadaJobConfig,
            conf
          )
      }
    }

    val executorsResponse =
      armadaClient.submitJobs(finalQueue, finalJobSetId, executors)
    val executorJobIds = executorsResponse.jobResponseItems.map(item => {
      log(
        s"Submitted executor job with ID: ${item.jobId}, Error: ${item.error}"
      )
      item.jobId
    })

    (driverJobId, executorJobIds)
  }

  /** Merges a driver job item template with runtime configuration.
    *
    * Precedence hierarchy (highest to lowest):
    *   1. Hardcoded values (always override): containers, podSpec fields like restartPolicy,
    *      terminationGracePeriodSeconds, services
    *   2. Individual runtime configuration: priority, namespace, specific labels/annotations
    *   3. Template values: all other fields from template
    *
    * @param template
    *   Driver job item template
    * @param armadaJobConfig
    *   Current job configuration
    * @param annotations
    *   Runtime annotations (merged with template)
    * @param labels
    *   Runtime labels (merged with template)
    * @param driverPort
    *   Driver port (hardcoded)
    * @param mainClass
    *   Main class (hardcoded)
    * @param volumes
    *   Volumes (hardcoded)
    * @param volumeMounts
    *   Volume mounts (hardcoded)
    * @param nodeSelectors
    *   Node selectors (hardcoded)
    * @param additionalDriverArgs
    *   Additional driver arguments (hardcoded)
    * @param conf
    *   Spark configuration
    * @return
    *   Merged JobSubmitRequestItem for driver
    */
  private def mergeDriverTemplate(
      template: api.submit.JobSubmitRequestItem,
      resolvedConfig: ResolvedConfig,
      armadaJobConfig: ArmadaJobConfig,
      annotations: Map[String, String],
      labels: Map[String, String],
      driverPort: Int,
      mainClass: String,
      volumes: Seq[Volume],
      volumeMounts: Seq[VolumeMount],
      nodeSelectors: Map[String, String],
      additionalDriverArgs: Seq[String],
      conf: SparkConf
  ): api.submit.JobSubmitRequestItem = {

    // Level 1: Hardcoded values (always override templates and config)
    val hardcodedContainer = newSparkDriverContainer(
      resolvedConfig.armadaClusterUrl,
      resolvedConfig.containerImage,
      driverPort,
      mainClass,
      volumeMounts,
      additionalDriverArgs,
      armadaJobConfig
    )

    val hardcodedPodSpec = template.podSpec
      .getOrElse(PodSpec())
      .withTerminationGracePeriodSeconds(0)    // Hardcoded
      .withRestartPolicy("Never")              // Hardcoded
      .withContainers(Seq(hardcodedContainer)) // Hardcoded
      .withVolumes(volumes)                    // Hardcoded
      .withNodeSelector(nodeSelectors)         // Hardcoded

    val hardcodedServices = Seq(
      api.submit.ServiceConfig(
        api.submit.ServiceType.Headless,
        Seq(driverPort)
      )
    )

    // Level 2: Individual runtime configuration overrides template
    val finalPriority  = armadaJobConfig.cliConfig.priority  // Runtime config overrides template
    val finalNamespace = armadaJobConfig.cliConfig.namespace // Runtime config overrides template

    // Level 3: Merge runtime and template values (runtime takes precedence)
    val mergedAnnotations = template.annotations ++ annotations
    val mergedLabels      = template.labels ++ labels

    // Apply precedence hierarchy: hardcoded > runtime config > template
    val resolvedPriority  = resolveValue(finalPriority, Some(template.priority), 0.0)
    val resolvedNamespace = resolveValue(finalNamespace, Some(template.namespace), "default")

    template
      .withPriority(resolvedPriority)     // Level 2: Runtime config
      .withNamespace(resolvedNamespace)   // Level 2: Runtime config
      .withLabels(mergedLabels)           // Level 3: Runtime + template merge
      .withAnnotations(mergedAnnotations) // Level 3: Runtime + template merge
      .withPodSpec(hardcodedPodSpec)      // Level 1: Hardcoded
      .withServices(hardcodedServices)    // Level 1: Hardcoded
  }

  /** Merges an executor job item template with runtime configuration.
    *
    * Precedence hierarchy (highest to lowest):
    *   1. Hardcoded values (always override): containers, initContainers, podSpec fields like
    *      restartPolicy, terminationGracePeriodSeconds
    *   2. Individual runtime configuration: priority, namespace, specific labels/annotations
    *   3. Template values: all other fields from template
    *
    * @param template
    *   Executor job item template
    * @param index
    *   Executor index (hardcoded)
    * @param armadaJobConfig
    *   Current job configuration
    * @param annotations
    *   Runtime annotations (merged with template)
    * @param labels
    *   Runtime labels (merged with template)
    * @param javaOptEnvVars
    *   Java option environment variables (hardcoded)
    * @param driverHostname
    *   Driver hostname (hardcoded)
    * @param driverPort
    *   Driver port (hardcoded)
    * @param volumes
    *   Volumes (hardcoded)
    * @param nodeSelectors
    *   Node selectors (hardcoded)
    * @param conf
    *   Spark configuration
    * @return
    *   Merged JobSubmitRequestItem for executor
    */
  private def mergeExecutorTemplate(
      template: api.submit.JobSubmitRequestItem,
      index: Int,
      armadaJobConfig: ArmadaJobConfig,
      annotations: Map[String, String],
      labels: Map[String, String],
      javaOptEnvVars: Seq[EnvVar],
      driverHostname: String,
      driverPort: Int,
      volumes: Seq[Volume],
      nodeSelectors: Map[String, String],
      conf: SparkConf
  ): api.submit.JobSubmitRequestItem = {

    // Level 1: Hardcoded values (always override templates and config)
    val resolvedTimeout = resolveValue(
      armadaJobConfig.cliConfig.executorConnectionTimeout,
      None,
      Duration(conf.get(ARMADA_EXECUTOR_CONNECTION_TIMEOUT), SECONDS)
    )
    val hardcodedInitContainer = newExecutorInitContainer(
      driverHostname,
      driverPort,
      resolvedTimeout
    )

    val resolvedContainerImage = resolveRequiredValue(
      armadaJobConfig.cliConfig.containerImage.filter(_.nonEmpty),
      None,
      s"Container image must be set via ${CONTAINER_IMAGE.key}"
    )
    val hardcodedContainer = newExecutorContainer(
      index,
      resolvedContainerImage,
      driverHostname,
      driverPort,
      armadaJobConfig.cliConfig.nodeUniformityLabel,
      javaOptEnvVars,
      armadaJobConfig,
      conf
    )

    val hardcodedPodSpec = template.podSpec
      .getOrElse(PodSpec())
      .withTerminationGracePeriodSeconds(0)            // Hardcoded
      .withRestartPolicy("Never")                      // Hardcoded
      .withInitContainers(Seq(hardcodedInitContainer)) // Hardcoded
      .withContainers(Seq(hardcodedContainer))         // Hardcoded
      .withVolumes(volumes)                            // Hardcoded
      .withNodeSelector(nodeSelectors)                 // Hardcoded

    // Level 2: Individual runtime configuration overrides template
    val finalPriority  = armadaJobConfig.cliConfig.priority  // Runtime config overrides template
    val finalNamespace = armadaJobConfig.cliConfig.namespace // Runtime config overrides template

    // Level 3: Merge runtime and template values (runtime takes precedence)
    val mergedAnnotations = template.annotations ++ annotations
    val mergedLabels      = template.labels ++ labels

    // Apply precedence hierarchy: hardcoded > runtime config > template
    val resolvedPriority2  = resolveValue(finalPriority, Some(template.priority), 0.0)
    val resolvedNamespace2 = resolveValue(finalNamespace, Some(template.namespace), "default")

    template
      .withPriority(resolvedPriority2)    // Level 2: Runtime config
      .withNamespace(resolvedNamespace2)  // Level 2: Runtime config
      .withLabels(mergedLabels)           // Level 3: Runtime + template merge
      .withAnnotations(mergedAnnotations) // Level 3: Runtime + template merge
      .withPodSpec(hardcodedPodSpec)      // Level 1: Hardcoded
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
      armadaJobConfig: ArmadaJobConfig
  ): api.submit.JobSubmitRequestItem = {
    val container = newSparkDriverContainer(
      master,
      driverImage,
      driverPort,
      mainClass,
      volumeMounts,
      additionalDriverArgs,
      armadaJobConfig
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
      armadaJobConfig: ArmadaJobConfig
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

    // Extract resources from template
    val templateResources = extractResourcesFromTemplate(armadaJobConfig.driverJobItemTemplate)

    // Apply precedence: CLI > Template > Default
    val driverLimits = Map(
      "memory" -> {
        armadaJobConfig.cliConfig.driverLimitMemory
          .map(value => Quantity(Option(value)))
          .orElse(templateResources.flatMap(_.limits.get("memory")))
          .getOrElse(Quantity(Option(DEFAULT_MEM)))
      },
      "cpu" -> {
        armadaJobConfig.cliConfig.driverLimitCores
          .map(value => Quantity(Option(value)))
          .orElse(templateResources.flatMap(_.limits.get("cpu")))
          .getOrElse(Quantity(Option(DEFAULT_CORES)))
      }
    ) ++ extractAdditionalTemplateResources(templateResources, "limits")

    val driverRequests = Map(
      "memory" -> {
        armadaJobConfig.cliConfig.driverRequestMemory
          .map(value => Quantity(Option(value)))
          .orElse(templateResources.flatMap(_.requests.get("memory")))
          .getOrElse(Quantity(Option(DEFAULT_MEM)))
      },
      "cpu" -> {
        armadaJobConfig.cliConfig.driverRequestCores
          .map(value => Quantity(Option(value)))
          .orElse(templateResources.flatMap(_.requests.get("cpu")))
          .getOrElse(Quantity(Option(DEFAULT_CORES)))
      }
    ) ++ extractAdditionalTemplateResources(templateResources, "requests")
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
      armadaJobConfig: ArmadaJobConfig,
      conf: SparkConf
  ): api.submit.JobSubmitRequestItem = {
    val initContainer = newExecutorInitContainer(driverHostname, driverPort, connectionTimeout)
    val container = newExecutorContainer(
      index,
      image,
      driverHostname,
      driverPort,
      nodeUniformityLabel,
      javaOptEnvVars,
      armadaJobConfig,
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
      armadaJobConfig: ArmadaJobConfig,
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

    // Extract resources from template
    val templateResources = extractResourcesFromTemplate(armadaJobConfig.executorJobItemTemplate)

    // Apply precedence: CLI > Template > Default
    val executorLimits = Map(
      "memory" -> {
        armadaJobConfig.cliConfig.executorLimitMemory
          .map(value => Quantity(Option(value)))
          .orElse(templateResources.flatMap(_.limits.get("memory")))
          .getOrElse(Quantity(Option(DEFAULT_MEM)))
      },
      "cpu" -> {
        armadaJobConfig.cliConfig.executorLimitCores
          .map(value => Quantity(Option(value)))
          .orElse(templateResources.flatMap(_.limits.get("cpu")))
          .getOrElse(Quantity(Option(DEFAULT_CORES)))
      }
    ) ++ extractAdditionalTemplateResources(templateResources, "limits")

    val executorRequests = Map(
      "memory" -> {
        armadaJobConfig.cliConfig.executorRequestMemory
          .map(value => Quantity(Option(value)))
          .orElse(templateResources.flatMap(_.requests.get("memory")))
          .getOrElse(Quantity(Option(DEFAULT_MEM)))
      },
      "cpu" -> {
        armadaJobConfig.cliConfig.executorRequestCores
          .map(value => Quantity(Option(value)))
          .orElse(templateResources.flatMap(_.requests.get("cpu")))
          .getOrElse(Quantity(Option(DEFAULT_CORES)))
      }
    ) ++ extractAdditionalTemplateResources(templateResources, "requests")
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

  /** Extracts resource values from a job item template's pod spec.
    *
    * @param template
    *   Optional job item template containing pod spec with resources
    * @return
    *   ResourceRequirements if found in template, None otherwise
    */
  private def extractResourcesFromTemplate(
      template: Option[api.submit.JobSubmitRequestItem]
  ): Option[ResourceRequirements] = {
    for {
      t         <- template
      podSpec   <- t.podSpec
      container <- podSpec.containers.headOption
      res       <- container.resources
    } yield res
  }

  /** Extracts additional resource types from template resources, excluding memory and CPU.
    *
    * This function filters out the standard memory and CPU resources that are handled explicitly
    * with CLI > Template > Default precedence, and returns all other resource types (like GPU,
    * ephemeral-storage, etc.) that are defined in the template.
    *
    * @param templateResources
    *   Optional resource requirements from a job item template
    * @param resourceType
    *   Either "limits" or "requests" to specify which resource map to extract from
    * @return
    *   Map of additional resource types (excluding memory/cpu) with their Quantity values
    */
  private def extractAdditionalTemplateResources(
      templateResources: Option[ResourceRequirements],
      resourceType: String
  ): Map[String, Quantity] = {
    val resourceMap = resourceType match {
      case "limits"   => templateResources.map(_.limits).getOrElse(Map.empty)
      case "requests" => templateResources.map(_.requests).getOrElse(Map.empty)
      case _ =>
        throw new IllegalArgumentException(
          s"Invalid resource type: $resourceType. Must be 'limits' or 'requests'"
        )
    }

    resourceMap.view
      .filterKeys(key => key != "memory" && key != "cpu")
      .toMap
  }
}
