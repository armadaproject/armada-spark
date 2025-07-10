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

import api.submit.JobSubmitRequestItem
import org.apache.spark.deploy.armada.Config.{
  ARMADA_AUTH_TOKEN,
  ARMADA_DRIVER_JOB_ITEM_TEMPLATE,
  ARMADA_DRIVER_LIMIT_CORES,
  ARMADA_DRIVER_LIMIT_MEMORY,
  ARMADA_DRIVER_REQUEST_CORES,
  ARMADA_DRIVER_REQUEST_MEMORY,
  ARMADA_EXECUTOR_CONNECTION_TIMEOUT,
  ARMADA_EXECUTOR_JOB_ITEM_TEMPLATE,
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
  ARMADA_LOOKOUTURL,
  ARMADA_RUN_AS_USER,
  ARMADA_SERVER_INTERNAL_URL,
  ARMADA_SPARK_DRIVER_LABELS,
  ARMADA_SPARK_EXECUTOR_LABELS,
  ARMADA_SPARK_JOB_NAMESPACE,
  ARMADA_SPARK_JOB_PRIORITY,
  ARMADA_SPARK_POD_LABELS,
  CONTAINER_IMAGE,
  DEFAULT_CORES,
  DEFAULT_MEM,
  DEFAULT_SPARK_EXECUTOR_CORES,
  DEFAULT_SPARK_EXECUTOR_MEMORY,
  commaSeparatedLabelsToMap
}
import io.armadaproject.armada.ArmadaClient
import k8s.io.api.core.v1.generated._
import k8s.io.apimachinery.pkg.api.resource.generated.Quantity
import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkApplication
import org.apache.spark.scheduler.cluster.SchedulerBackendUtils
import org.json4s.jackson.JsonMethods.{parse, pretty, render}
import org.json4s.JsonDSL._
import org.json4s.{DefaultFormats, Extraction, JValue}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.JsonNode
import io.fabric8.kubernetes.client.utils.Serialization
import org.apache.spark.deploy.k8s.submit.KubernetesDriverBuilder
import org.apache.spark.deploy.k8s.submit.{PythonMainAppResource => KPMainAppResource}
import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration._

import io.fabric8.kubernetes.client.DefaultKubernetesClient
import org.apache.spark.deploy.k8s.{KubernetesDriverConf, KubernetesExecutorConf}
import org.apache.spark.SecurityManager
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.scheduler.cluster.k8s.KubernetesExecutorBuilder

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

private[spark] object ArmadaClientApplication {
  private[submit] val DRIVER_PORT = 7078
  private val DEFAULT_PRIORITY    = 0.0
  private val DEFAULT_NAMESPACE   = "default"
  private val DEFAULT_RUN_AS_USER = 185

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

  implicit val formats     = DefaultFormats
  private val objectMapper = new ObjectMapper()

  override def start(args: Array[String], conf: SparkConf): Unit = {
    log("ArmadaClientApplication: starting.")
    val parsedArguments = ClientArguments.fromCommandLineArgs(args)
    run(parsedArguments, conf)
  }

  private def run(
      clientArguments: ClientArguments,
      sparkConf: SparkConf
  ): Unit = {
    val armadaJobConfig = validateArmadaJobConfig(sparkConf, clientArguments)

    val (host, port) = ArmadaUtils.parseMasterUrl(sparkConf.get("spark.master"))
    log(s"host is $host, port is $port")

    val driverSpec = new KubernetesDriverBuilder().buildFromFeatures(
      new KubernetesDriverConf(
        sparkConf = sparkConf.clone(),
        appId = "",
        mainAppResource = KPMainAppResource("/opt/spark/examples/src/main/python/pi.py"),
        mainClass = clientArguments.mainClass,
        appArgs = Array("100"),
        proxyUser = None
      ),
      null
    )
    println("gbjD: " + driverSpec)
    val yamlString = Serialization.asYaml(driverSpec)
    println("gbjyamlDriver: " + yamlString)
    val armadaClient = ArmadaClient(host, port, false, sparkConf.get(ARMADA_AUTH_TOKEN))
    val healthTimeout =
      Duration(sparkConf.get(ARMADA_HEALTH_CHECK_TIMEOUT), SECONDS)
    // val healthResp = Await.result(armadaClient.submitHealth(), healthTimeout)

    // if (healthResp.status.isServing) {
    //   log("Submit health good!")
    // } else {
    //   log("Could not contact Armada!")
    // }

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

  private[spark] def validateArmadaJobConfig(
      conf: SparkConf,
      clientArguments: ClientArguments = null
  ): ArmadaJobConfig = {
    val jobTemplate: Option[api.submit.JobSubmitRequest] = conf
      .get(ARMADA_JOB_TEMPLATE)
      .filter(_.nonEmpty)
      .map(JobTemplateLoader.loadJobTemplate)

    val driverJobItemTemplate: Option[api.submit.JobSubmitRequestItem] = conf
      .get(ARMADA_DRIVER_JOB_ITEM_TEMPLATE)
      .filter(_.nonEmpty)
      .map(JobTemplateLoader.loadJobItemTemplate)

    val executorJobItemTemplate: Option[api.submit.JobSubmitRequestItem] = conf
      .get(ARMADA_EXECUTOR_JOB_ITEM_TEMPLATE)
      .filter(_.nonEmpty)
      .map(JobTemplateLoader.loadJobItemTemplate)

    val cliConfig = parseCLIConfig(conf)

    validateRequiredConfig(
      cliConfig,
      jobTemplate,
      driverJobItemTemplate,
      executorJobItemTemplate,
      conf
    )

    val finalQueue = cliConfig.queue
      .filter(_.nonEmpty)
      .orElse(jobTemplate.map(_.queue).filter(_.nonEmpty))
      .get // Safe to use .get because validation ensures queue exists

    val finalJobSetId = cliConfig.jobSetId
      .filter(_.nonEmpty)
      .orElse(jobTemplate.map(_.jobSetId).filter(_.nonEmpty))
      .getOrElse(conf.getAppId)

    val (driverPodSpec, driverContainer)     = getDriverFeatureSteps(conf, clientArguments)
    val (executorPodSpec, executorContainer) = getExecutorFeatureSteps(conf, clientArguments)
    ArmadaJobConfig(
      queue = finalQueue,
      jobSetId = finalJobSetId,
      jobTemplate = jobTemplate,
      driverJobItemTemplate = driverJobItemTemplate,
      executorJobItemTemplate = executorJobItemTemplate,
      cliConfig = cliConfig,
      driverFeatureStepPodSpec = driverPodSpec,
      driverFeatureStepContainer = driverContainer,
      executorFeatureStepPodSpec = executorPodSpec,
      executorFeatureStepContainer = executorContainer
    )
  }

  private def getExecutorFeatureSteps(conf: SparkConf, clientArguments: ClientArguments) = {
    val executorConf = new KubernetesExecutorConf(
      sparkConf = conf.clone(),
      appId = "appId",
      executorId = "execId",
      driverPod = None,
      resourceProfileId = 1
    )
    val executorSpec = new KubernetesExecutorBuilder().buildFromFeatures(
      executorConf,
      new SecurityManager(conf),
      new DefaultKubernetesClient(),
      new ResourceProfile(executorResources = null, taskResources = null)
    )
    val execPodString = Serialization.asYaml(executorSpec.pod.pod)
    val execPod: k8s.io.api.core.v1.generated.Pod =
      JobTemplateLoader.unmarshal(execPodString, classOf[Pod], "executor pod")


    executorSpec.pod.container.setVolumeMounts(null)

    val execContainerString = Serialization.asYaml(executorSpec.pod.container)
    val execContainer: k8s.io.api.core.v1.generated.Container =
      JobTemplateLoader.unmarshal(execContainerString, classOf[Container], "executor container")
    (Some(execPod.getSpec), Some(execContainer))
  }

  private def getDriverFeatureSteps(conf: SparkConf, clientArguments: ClientArguments) = {

    val driverSpec = new KubernetesDriverBuilder().buildFromFeatures(
      new KubernetesDriverConf(
        sparkConf = conf.clone(),
        appId = "",
        mainAppResource = KPMainAppResource("/opt/spark/examples/src/main/python/pi.py"),
        mainClass = clientArguments.mainClass,
        appArgs = Array("100"),
        proxyUser = None
      ),
      new DefaultKubernetesClient()
    )
    val yamlString = Serialization.asYaml(driverSpec.pod.pod)
    val pod: k8s.io.api.core.v1.generated.Pod =
      JobTemplateLoader.unmarshal(yamlString, classOf[Pod], "driver")

    driverSpec.pod.container.setVolumeMounts(null)

    val containerString = Serialization.asYaml(driverSpec.pod.container)
    val container: k8s.io.api.core.v1.generated.Container = {
      JobTemplateLoader.unmarshal(containerString, classOf[Container], "driver")
    }
    val newArgs = container.args
      .map(arg => if (arg.contains("spark-upload"))
        "/opt/spark/examples/src/main/python/pi.py" else arg )
        (Some(pod.getSpec), Some(container.withArgs(newArgs)))
  }

  private[submit] def submitArmadaJob(
      armadaClient: ArmadaClient,
      clientArguments: ClientArguments,
      armadaJobConfig: ArmadaJobConfig,
      conf: SparkConf
  ): (String, Seq[String]) = {

    val primaryResource = extractPrimaryResource(clientArguments.mainAppResource)
    val executorCount   = SchedulerBackendUtils.getInitialTargetExecutorNumber(conf)

    if (executorCount <= 0) {
      throw new IllegalArgumentException(
        s"Executor count must be greater than 0, but got: $executorCount"
      )
    }

    val confSeq         = buildSparkConfArgs(conf)
    val configGenerator = new ConfigGenerator("armada-spark-config", conf)

    val (templateAnnotations, templateLabels) = extractTemplateMetadata(armadaJobConfig.jobTemplate)
    val runtimeAnnotations = buildAnnotations(
      configGenerator,
      templateAnnotations,
      armadaJobConfig.cliConfig.nodeUniformityLabel,
      executorCount
    )
    val runtimeLabels = buildLabels(
      armadaJobConfig.cliConfig.podLabels,
      templateLabels,
      armadaJobConfig.cliConfig.driverLabels
    )

    val resolvedConfig = resolveJobConfig(
      armadaJobConfig.cliConfig,
      armadaJobConfig.driverJobItemTemplate,
      runtimeAnnotations,
      runtimeLabels,
      conf
    )

    val driver = createDriverJob(
      armadaJobConfig,
      resolvedConfig,
      configGenerator,
      clientArguments,
      primaryResource,
      confSeq
    )
    val driverJobId =
      submitDriver(armadaClient, armadaJobConfig.queue, armadaJobConfig.jobSetId, driver)

    val executorLabels = buildLabels(
      armadaJobConfig.cliConfig.podLabels,
      templateLabels,
      armadaJobConfig.cliConfig.executorLabels
    )

    val executorResolvedConfig = resolveJobConfig(
      armadaJobConfig.cliConfig,
      armadaJobConfig.executorJobItemTemplate,
      runtimeAnnotations,
      executorLabels,
      conf
    )

    val driverHostname = ArmadaUtils.buildServiceNameFromJobId(driverJobId)
    val executors = createExecutorJobs(
      armadaJobConfig,
      executorResolvedConfig,
      configGenerator,
      driverHostname,
      executorCount,
      conf
    )
    val executorJobIds = submitExecutors(
      armadaClient,
      armadaJobConfig.queue,
      armadaJobConfig.jobSetId,
      executors
    )

    (driverJobId, executorJobIds)
  }

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
      runAsUser: Option[Long],
      driverResources: ResourceConfig,
      executorResources: ResourceConfig
  )

  /** Resource configuration for a pod (driver or executor).
    *
    * @param limitCores
    *   CPU limit for the pod
    * @param requestCores
    *   CPU request for the pod
    * @param limitMemory
    *   Memory limit for the pod
    * @param requestMemory
    *   Memory request for the pod
    */
  private[spark] case class ResourceConfig(
      limitCores: Option[String],
      requestCores: Option[String],
      limitMemory: Option[String],
      requestMemory: Option[String]
  )

  private[spark] def parseCLIConfig(conf: SparkConf): CLIConfig = {
    // Only extract CLI values, don't apply defaults or template values here
    // Note: Don't filter empty strings here - validation should handle that later
    val queue          = conf.get(ARMADA_JOB_QUEUE)
    val jobSetId       = conf.get(ARMADA_JOB_SET_ID)
    val runAsUser      = conf.get(ARMADA_RUN_AS_USER)
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

    val driverResources = ResourceConfig(
      limitCores = conf.get(ARMADA_DRIVER_LIMIT_CORES),
      requestCores = conf.get(ARMADA_DRIVER_REQUEST_CORES),
      limitMemory = conf.get(ARMADA_DRIVER_LIMIT_MEMORY),
      requestMemory = conf.get(ARMADA_DRIVER_REQUEST_MEMORY)
    )

    val executorResources = ResourceConfig(
      limitCores = conf.get(ARMADA_EXECUTOR_LIMIT_CORES),
      requestCores = conf.get(ARMADA_EXECUTOR_REQUEST_CORES),
      limitMemory = conf.get(ARMADA_EXECUTOR_LIMIT_MEMORY),
      requestMemory = conf.get(ARMADA_EXECUTOR_REQUEST_MEMORY)
    )

    CLIConfig(
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
      armadaClusterUrl = Some(armadaClusterUrl),
      executorConnectionTimeout =
        Some(Duration(conf.get(ARMADA_EXECUTOR_CONNECTION_TIMEOUT), SECONDS)),
      runAsUser = runAsUser,
      driverResources = driverResources,
      executorResources = executorResources
    )
  }

  /** Validates required configuration values and throws IllegalArgumentException if invalid.
    *
    * @param cliConfig
    *   CLI configuration parsed from --conf options
    * @param jobTemplate
    *   Optional job template for validation
    * @param driverJobItemTemplate
    *   Optional driver job item template for validation
    * @param executorJobItemTemplate
    *   Optional executor job item template for validation
    * @param conf
    *   Spark configuration
    * @throws IllegalArgumentException
    *   if required values are missing or invalid
    */
  private[submit] def validateRequiredConfig(
      cliConfig: CLIConfig,
      jobTemplate: Option[api.submit.JobSubmitRequest],
      driverJobItemTemplate: Option[api.submit.JobSubmitRequestItem],
      executorJobItemTemplate: Option[api.submit.JobSubmitRequestItem],
      conf: SparkConf
  ): Unit = {
    validateContainerImage(cliConfig, driverJobItemTemplate, executorJobItemTemplate)

    val hasValidQueue = cliConfig.queue.exists(_.nonEmpty) ||
      jobTemplate.exists(_.queue.nonEmpty)
    if (!hasValidQueue) {
      throw new IllegalArgumentException(
        s"Queue name must be set via ${ARMADA_JOB_QUEUE.key} or in job template."
      )
    }

    if (cliConfig.jobSetId.exists(_.isEmpty)) {
      throw new IllegalArgumentException(
        s"Empty jobSetId is not allowed. Please set a valid jobSetId via ${ARMADA_JOB_SET_ID.key}"
      )
    }
  }

  /** Comprehensive resolved configuration holding all resolved values after applying precedence */
  private[submit] case class ResolvedJobConfig(
      namespace: String,
      priority: Double,
      containerImage: String,
      armadaClusterUrl: String,
      executorConnectionTimeout: Duration,
      annotations: Map[String, String],
      labels: Map[String, String],
      nodeSelectors: Map[String, String],
      runAsUser: Long,
      driverResources: ResolvedResourceConfig,
      executorResources: ResolvedResourceConfig
  )

  /** Resolved resource configuration for driver and executor pods */
  private[submit] case class ResolvedResourceConfig(
      limitCores: Option[String],
      requestCores: Option[String],
      limitMemory: Option[String],
      requestMemory: Option[String]
  )

  /** Resolves all configuration values by applying precedence hierarchy.
    *
    * @param cliConfig
    *   CLI configuration values
    * @param template
    *   Optional template containing default values
    * @param annotations
    *   Runtime annotations to merge with template
    * @param labels
    *   Runtime labels to merge with template
    * @param conf
    *   Spark configuration for defaults
    * @return
    *   Comprehensive resolved configuration
    */
  private[submit] def resolveJobConfig(
      cliConfig: CLIConfig,
      template: Option[api.submit.JobSubmitRequestItem],
      annotations: Map[String, String],
      labels: Map[String, String],
      conf: SparkConf
  ): ResolvedJobConfig = {
    val templateAnnotations = template.map(_.annotations).getOrElse(Map.empty)
    val templateLabels      = template.map(_.labels).getOrElse(Map.empty)

    val mergedAnnotations = templateAnnotations ++ annotations
    val mergedLabels      = templateLabels ++ labels

    val resolvedPriority = resolveValue(
      cliConfig.priority,
      template.map(_.priority),
      ArmadaClientApplication.DEFAULT_PRIORITY
    )
    val resolvedNamespace = resolveValue(
      cliConfig.namespace,
      template.map(_.namespace),
      ArmadaClientApplication.DEFAULT_NAMESPACE
    )

    val resolvedRunAsUser: Long = resolveValue(
      cliConfig.runAsUser,
      extractRunAsUserFromTemplate(template),
      ArmadaClientApplication.DEFAULT_RUN_AS_USER
    )

    val containerImage = cliConfig.containerImage
      .orElse(extractContainerImageFromTemplate(template))
      .get // Safe to use .get because validation ensures container image exists
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

    ResolvedJobConfig(
      namespace = resolvedNamespace,
      priority = resolvedPriority,
      containerImage = containerImage,
      armadaClusterUrl = armadaClusterUrl,
      executorConnectionTimeout = executorConnectionTimeout,
      annotations = mergedAnnotations,
      labels = mergedLabels,
      nodeSelectors = cliConfig.nodeSelectors,
      runAsUser = resolvedRunAsUser,
      driverResources = ResolvedResourceConfig(
        cliConfig.driverResources.limitCores,
        cliConfig.driverResources.requestCores,
        cliConfig.driverResources.limitMemory,
        cliConfig.driverResources.requestMemory
      ),
      executorResources = ResolvedResourceConfig(
        cliConfig.executorResources.limitCores,
        cliConfig.executorResources.requestCores,
        cliConfig.executorResources.limitMemory,
        cliConfig.executorResources.requestMemory
      )
    )
  }

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
      queue: String,
      jobSetId: String,
      jobTemplate: Option[api.submit.JobSubmitRequest],
      driverJobItemTemplate: Option[api.submit.JobSubmitRequestItem],
      executorJobItemTemplate: Option[api.submit.JobSubmitRequestItem],
      cliConfig: CLIConfig,
      driverFeatureStepPodSpec: Option[PodSpec] = None,
      driverFeatureStepContainer: Option[Container] = None,
      executorFeatureStepPodSpec: Option[PodSpec] = None,
      executorFeatureStepContainer: Option[Container] = None
  )

  private[submit] def createDriverJob(
      armadaJobConfig: ArmadaJobConfig,
      resolvedConfig: ResolvedJobConfig,
      configGenerator: ConfigGenerator,
      clientArguments: ClientArguments,
      primaryResource: Seq[String],
      confSeq: Seq[String]
  ): api.submit.JobSubmitRequestItem = {
    val driverArgs = confSeq ++ primaryResource ++ clientArguments.driverArgs

    mergeDriverTemplate(
      armadaJobConfig.driverJobItemTemplate,
      resolvedConfig,
      armadaJobConfig,
      ArmadaClientApplication.DRIVER_PORT,
      clientArguments.mainClass,
      configGenerator.getVolumes,
      configGenerator.getVolumeMounts,
      driverArgs
    )
  }

  private[submit] def createExecutorJobs(
      armadaJobConfig: ArmadaJobConfig,
      resolvedConfig: ResolvedJobConfig,
      configGenerator: ConfigGenerator,
      driverHostname: String,
      executorCount: Int,
      conf: SparkConf
  ): Seq[api.submit.JobSubmitRequestItem] = {
    ArmadaUtils.getExecutorRange(executorCount).map { index =>
      mergeExecutorTemplate(
        armadaJobConfig.executorJobItemTemplate,
        index,
        resolvedConfig,
        armadaJobConfig,
        javaOptEnvVars(conf),
        driverHostname,
        ArmadaClientApplication.DRIVER_PORT,
        configGenerator.getVolumes,
        conf
      )
    }
  }

  private def submitDriver(
      armadaClient: ArmadaClient,
      queue: String,
      jobSetId: String,
      driver: api.submit.JobSubmitRequestItem
  ): String = {
    val driverResponse = armadaClient.submitJobs(queue, jobSetId, Seq(driver))
    val driverJobId    = driverResponse.jobResponseItems.head.jobId
    val error =
      if (driverResponse.jobResponseItems.head.error.nonEmpty)
        driverResponse.jobResponseItems.head.error
      else "none"
    log(
      s"Submitted driver job with ID: $driverJobId, Error: $error"
    )
    driverJobId
  }

  private def submitExecutors(
      armadaClient: ArmadaClient,
      queue: String,
      jobSetId: String,
      executors: Seq[api.submit.JobSubmitRequestItem]
  ): Seq[String] = {
    val executorsResponse = armadaClient.submitJobs(queue, jobSetId, executors)
    executorsResponse.jobResponseItems.map { item =>
      val error = if (item.error.nonEmpty) item.error else "none"
      log(s"Submitted executor job with ID: ${item.jobId}, Error: $error")
      item.jobId
    }
  }

  /** Merges a driver job item template with runtime configuration. If no template is provided,
    * creates a blank template first.
    *
    * CLI configuration flags override template values. Some fields (containers, services,
    * restartPolicy, terminationGracePeriodSeconds) are always overridden to ensure correct Spark
    * behavior.
    */
  private[submit] def mergeDriverTemplate(
      template: Option[api.submit.JobSubmitRequestItem],
      resolvedConfig: ResolvedJobConfig,
      armadaJobConfig: ArmadaJobConfig,
      driverPort: Int,
      mainClass: String,
      volumes: Seq[Volume],
      volumeMounts: Seq[VolumeMount],
      additionalDriverArgs: Seq[String]
  ): api.submit.JobSubmitRequestItem = {

    val workingTemplate = getWorkingTemplate(template)
    workingTemplate.withPodSpec(armadaJobConfig.driverFeatureStepPodSpec.get)

    val (templateVolumes, templateVolumeMounts) = extractTemplateVolumesAndMounts(workingTemplate)
    val mergedVolumes                           = templateVolumes ++ volumes
    val mergedVolumeMounts                      = templateVolumeMounts ++ volumeMounts

    val finalContainer = newDriverContainer(
      resolvedConfig.armadaClusterUrl,
      resolvedConfig.containerImage,
      driverPort,
      mainClass,
      mergedVolumeMounts,
      additionalDriverArgs,
      armadaJobConfig
    )

    val templateNodeSelectors = workingTemplate.podSpec.map(_.nodeSelector).getOrElse(Map.empty)
    val mergedNodeSelectors = if (resolvedConfig.nodeSelectors.nonEmpty) {
      resolvedConfig.nodeSelectors
    } else {
      templateNodeSelectors
    }

    val finalPodSpec = workingTemplate.podSpec
      .getOrElse(PodSpec())
      .withTerminationGracePeriodSeconds(0)
      .withRestartPolicy("Never")
      .withContainers(Seq(finalContainer))
      .withVolumes(mergedVolumes)
      .withNodeSelector(mergedNodeSelectors)
      .withSecurityContext(new PodSecurityContext().withRunAsUser(resolvedConfig.runAsUser))

//    log(s"Driver pod spec: ${pretty(render(Extraction.decompose(finalPodSpec)))}")

    val jsonString =
      """{"containers":[{"image":"nginx:1.21","name":"nginx","ports":[{"containerPort":80,"name":"http"}],"resources":{"limits":{"cpu":"200m","memory":"256Mi"},"requests":{"cpu":"100m","memory":"128Mi"}}}],"restartPolicy":"Always"}
                       |""".stripMargin

    val finalServices = Seq(
      api.submit.ServiceConfig(
        api.submit.ServiceType.Headless,
        Seq(driverPort)
      )
    )

    workingTemplate
      .withPriority(resolvedConfig.priority)
      .withNamespace(resolvedConfig.namespace)
      .withLabels(resolvedConfig.labels)
      .withAnnotations(resolvedConfig.annotations)
      .withPodSpec(finalPodSpec)
      .withServices(finalServices)
  }

  // Create a blank template if none provided
  private def getWorkingTemplate(template: Option[JobSubmitRequestItem]): JobSubmitRequestItem = {
    template.getOrElse {
      api.submit
        .JobSubmitRequestItem()
        .withPriority(ArmadaClientApplication.DEFAULT_PRIORITY)
        .withNamespace(ArmadaClientApplication.DEFAULT_NAMESPACE)
        .withLabels(Map.empty)
        .withAnnotations(Map.empty)
        .withPodSpec(
          PodSpec()
            .withNodeSelector(Map.empty)
        )
    }
  }

  /** Merges an executor job item template with runtime configuration. If no template is provided,
    * creates a blank template first.
    */
  private[submit] def mergeExecutorTemplate(
      template: Option[api.submit.JobSubmitRequestItem],
      index: Int,
      resolvedConfig: ResolvedJobConfig,
      armadaJobConfig: ArmadaJobConfig,
      javaOptEnvVars: Seq[EnvVar],
      driverHostname: String,
      driverPort: Int,
      volumes: Seq[Volume],
      conf: SparkConf
  ): api.submit.JobSubmitRequestItem = {

    val workingTemplate = getWorkingTemplate(template)
    workingTemplate.withPodSpec(armadaJobConfig.executorFeatureStepPodSpec.get)

    val resolvedTimeout = resolvedConfig.executorConnectionTimeout
    val finalInitContainer = newExecutorInitContainer(
      driverHostname,
      driverPort,
      resolvedTimeout
    )

    val (templateVolumes, templateVolumeMounts) = extractTemplateVolumesAndMounts(workingTemplate)
    val mergedVolumes                           = templateVolumes ++ volumes

    val baseContainer = newExecutorContainer(
      index,
      resolvedConfig.containerImage,
      driverHostname,
      driverPort,
      armadaJobConfig.cliConfig.nodeUniformityLabel,
      javaOptEnvVars,
      armadaJobConfig,
      conf
    )

    val finalContainer = baseContainer.withVolumeMounts(
      baseContainer.volumeMounts ++ templateVolumeMounts
    )

    val templateNodeSelectors = workingTemplate.podSpec.map(_.nodeSelector).getOrElse(Map.empty)
    val mergedNodeSelectors = if (resolvedConfig.nodeSelectors.nonEmpty) {
      resolvedConfig.nodeSelectors
    } else {
      templateNodeSelectors
    }

    val finalPodSpec = workingTemplate.podSpec
      .getOrElse(PodSpec())
      .withTerminationGracePeriodSeconds(0)
      .withRestartPolicy("Never")
      .withInitContainers(Seq(finalInitContainer))
      .withContainers(Seq(finalContainer))
      .withVolumes(mergedVolumes)
      .withNodeSelector(mergedNodeSelectors)
      .withSecurityContext(new PodSecurityContext().withRunAsUser(resolvedConfig.runAsUser))

    workingTemplate
      .withPriority(resolvedConfig.priority)
      .withNamespace(resolvedConfig.namespace)
      .withLabels(resolvedConfig.labels)
      .withAnnotations(resolvedConfig.annotations)
      .withPodSpec(finalPodSpec)
  }

  private def newDriverContainer(
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

    val templateResources = extractResourcesFromTemplate(armadaJobConfig.driverJobItemTemplate)

    val driverLimits = Map(
      "memory" -> {
        armadaJobConfig.cliConfig.driverResources.limitMemory
          .map(value => Quantity(Option(value)))
          .orElse(templateResources.flatMap(_.limits.get("memory")))
          .getOrElse(Quantity(Option(DEFAULT_MEM)))
      },
      "cpu" -> {
        armadaJobConfig.cliConfig.driverResources.limitCores
          .map(value => Quantity(Option(value)))
          .orElse(templateResources.flatMap(_.limits.get("cpu")))
          .getOrElse(Quantity(Option(DEFAULT_CORES)))
      }
    ) ++ extractAdditionalTemplateResources(templateResources, "limits")

    val driverRequests = Map(
      "memory" -> {
        armadaJobConfig.cliConfig.driverResources.requestMemory
          .map(value => Quantity(Option(value)))
          .orElse(templateResources.flatMap(_.requests.get("memory")))
          .getOrElse(Quantity(Option(DEFAULT_MEM)))
      },
      "cpu" -> {
        armadaJobConfig.cliConfig.driverResources.requestCores
          .map(value => Quantity(Option(value)))
          .orElse(templateResources.flatMap(_.requests.get("cpu")))
          .getOrElse(Quantity(Option(DEFAULT_CORES)))
      }
    ) ++ extractAdditionalTemplateResources(templateResources, "requests")

    armadaJobConfig.driverFeatureStepContainer.get
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
      .withVolumeMounts(volumeMounts)
      .withResources(
        ResourceRequirements(
          requests = driverRequests,
          limits = driverLimits
        )
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

    val templateResources = extractResourcesFromTemplate(armadaJobConfig.executorJobItemTemplate)

    val executorLimits = Map(
      "memory" -> {
        armadaJobConfig.cliConfig.executorResources.limitMemory
          .map(value => Quantity(Option(value)))
          .orElse(templateResources.flatMap(_.limits.get("memory")))
          .getOrElse(Quantity(Option(DEFAULT_MEM)))
      },
      "cpu" -> {
        armadaJobConfig.cliConfig.executorResources.limitCores
          .map(value => Quantity(Option(value)))
          .orElse(templateResources.flatMap(_.limits.get("cpu")))
          .getOrElse(Quantity(Option(DEFAULT_CORES)))
      }
    ) ++ extractAdditionalTemplateResources(templateResources, "limits")

    val executorRequests = Map(
      "memory" -> {
        armadaJobConfig.cliConfig.executorResources.requestMemory
          .map(value => Quantity(Option(value)))
          .orElse(templateResources.flatMap(_.requests.get("memory")))
          .getOrElse(Quantity(Option(DEFAULT_MEM)))
      },
      "cpu" -> {
        armadaJobConfig.cliConfig.executorResources.requestCores
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
    armadaJobConfig.executorFeatureStepContainer.get
      .withEnv(envVars ++ javaOptEnvVars)
      .withResources(
        ResourceRequirements(
          requests = executorRequests,
          limits = executorLimits
        )
      )
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
  private[submit] def resolveValue[T](
      cliValue: Option[T],
      templateValue: => Option[T],
      defaultValue: => T
  ): T = {
    cliValue.orElse(templateValue).getOrElse(defaultValue)
  }

  // Helper method to extract volumes and volumeMounts from a template
  private def extractTemplateVolumesAndMounts(
      template: api.submit.JobSubmitRequestItem
  ): (Seq[Volume], Seq[VolumeMount]) = {
    val templateVolumes = template.podSpec.map(_.volumes).getOrElse(Seq.empty)
    val templateVolumeMounts = template.podSpec
      .flatMap(_.containers.headOption)
      .map(_.volumeMounts)
      .getOrElse(Seq.empty)
    (templateVolumes, templateVolumeMounts)
  }

  // Helper method to extract container image from a template
  private def extractContainerImageFromTemplate(
      template: Option[api.submit.JobSubmitRequestItem]
  ): Option[String] = {
    for {
      t         <- template
      podSpec   <- t.podSpec
      container <- podSpec.containers.headOption
      image     <- container.image
    } yield image
  }

  private def extractRunAsUserFromTemplate(
      template: Option[api.submit.JobSubmitRequestItem]
  ): Option[Long] = {
    for {
      t               <- template
      podSpec         <- t.podSpec
      securityContext <- podSpec.securityContext
      runAsUser       <- securityContext.runAsUser
    } yield runAsUser
  }

  // Validates that container image is provided either via CLI or in both templates
  private def validateContainerImage(
      cliConfig: CLIConfig,
      driverTemplate: Option[api.submit.JobSubmitRequestItem],
      executorTemplate: Option[api.submit.JobSubmitRequestItem]
  ): Unit = {
    cliConfig.containerImage match {
      case Some(image) if image.isEmpty =>
        throw new IllegalArgumentException(
          s"Empty container image is not allowed. Please set a valid container image via ${CONTAINER_IMAGE.key}"
        )
      case None =>
        val driverImage   = extractContainerImageFromTemplate(driverTemplate)
        val executorImage = extractContainerImageFromTemplate(executorTemplate)

        if (driverImage.isEmpty || executorImage.isEmpty) {
          throw new IllegalArgumentException(
            s"Container image must be set via ${CONTAINER_IMAGE.key} or provided in BOTH driver and executor job item templates " +
              s"(found driver: ${driverImage.isDefined}, executor: ${executorImage.isDefined})"
          )
        }
      case Some(_) => // Valid non-empty image provided via CLI
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
    }.toVector
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

    resourceMap.filter { case (key, _) => key != "memory" && key != "cpu" }
  }

  private def extractPrimaryResource(mainAppResource: MainAppResource): Seq[String] = {
    mainAppResource match {
      case JavaMainAppResource(Some(resource)) => Seq(resource)
      case PythonMainAppResource(resource)     => Seq(resource)
      case RMainAppResource(resource)          => Seq(resource)
      case _                                   => Seq()
    }
  }

  private def buildSparkConfArgs(conf: SparkConf): Seq[String] = {
    conf.getAll.flatMap { case (k, v) =>
      Seq("--conf", s"$k=$v")
    }
  }

  private def extractTemplateMetadata(
      jobTemplate: Option[api.submit.JobSubmitRequest]
  ): (Map[String, String], Map[String, String]) = {
    jobTemplate match {
      case Some(template) =>
        template.jobRequestItems.headOption match {
          case Some(firstItem) => (firstItem.annotations, firstItem.labels)
          case None            => (Map.empty, Map.empty)
        }
      case None => (Map.empty, Map.empty)
    }
  }

  private def buildAnnotations(
      configGenerator: ConfigGenerator,
      templateAnnotations: Map[String, String],
      nodeUniformityLabel: Option[String],
      executorCount: Int
  ): Map[String, String] = {
    configGenerator.getAnnotations ++ templateAnnotations ++ nodeUniformityLabel
      .map(label =>
        GangSchedulingAnnotations(
          None,
          1 + executorCount,
          label
        )
      )
      .getOrElse(Map.empty)
  }

  private def buildLabels(
      podLabels: Map[String, String],
      templateLabels: Map[String, String],
      roleSpecificLabels: Map[String, String]
  ): Map[String, String] = {
    podLabels ++ templateLabels ++ roleSpecificLabels
  }
}
