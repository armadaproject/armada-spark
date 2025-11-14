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
  ARMADA_EXECUTOR_INIT_CONTAINER_CPU,
  ARMADA_EXECUTOR_INIT_CONTAINER_IMAGE,
  ARMADA_EXECUTOR_INIT_CONTAINER_MEMORY,
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
  ARMADA_SPARK_DRIVER_INGRESS_ANNOTATIONS,
  ARMADA_SPARK_DRIVER_INGRESS_CERT_NAME,
  ARMADA_SPARK_DRIVER_INGRESS_ENABLED,
  ARMADA_SPARK_DRIVER_INGRESS_PORT,
  ARMADA_SPARK_DRIVER_INGRESS_TLS_ENABLED,
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
  commaSeparatedAnnotationsToMap,
  commaSeparatedLabelsToMap
}
import org.apache.spark.deploy.armada.ModeHelper
import io.armadaproject.armada.ArmadaClient
import k8s.io.api.core.v1.generated._
import k8s.io.apimachinery.pkg.api.resource.generated.Quantity
import org.apache.spark.deploy.SparkApplication
import org.apache.spark.deploy.k8s.submit.{
  JavaMainAppResource,
  KubernetesDriverBuilder,
  MainAppResource,
  PythonMainAppResource,
  RMainAppResource
}
import org.apache.spark.deploy.k8s.{KubernetesDriverConf, KubernetesExecutorConf}
import org.apache.spark.deploy.k8s.Config.{CONTAINER_IMAGE => KUBERNETES_CONTAINER_IMAGE}
import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.scheduler.cluster.SchedulerBackendUtils
import org.apache.spark.scheduler.cluster.k8s.KubernetesExecutorBuilder
import io.fabric8.kubernetes.client.DefaultKubernetesClient

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.concurrent.Await
import scala.concurrent.duration._
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

private[spark] object ArmadaClientApplication {
  private[submit] val DRIVER_PORT = 7078
  private val DEFAULT_PRIORITY    = 0.0
  private val DEFAULT_NAMESPACE   = "default"
  private val DEFAULT_ARMADA_APP_ID =
    s"armada-spark-app-id-${UUID.randomUUID().toString.replaceAll("-", "")}"
  private val DEFAULT_RUN_AS_USER = 185

  val gangId = Some(java.util.UUID.randomUUID.toString)

}

/** Main class and entry point of application submission in KUBERNETES mode.
  */
private[spark] class ArmadaClientApplication extends SparkApplication {

  private def getApplicationId(conf: SparkConf) =
    conf.getOption("spark.app.id").getOrElse(ArmadaClientApplication.DEFAULT_ARMADA_APP_ID)

  private def log(msg: String): Unit = {
    // scalastyle:off println
    System.err.println(msg)
    // scalastyle:on println
  }

  override def start(args: Array[String], conf: SparkConf): Unit = {
    val parsedArguments = ClientArguments.fromCommandLineArgs(args)
    run(parsedArguments, conf)
  }

  private def run(
      clientArguments: ClientArguments,
      sparkConf: SparkConf
  ): Unit = {
    val armadaJobConfig = validateArmadaJobConfig(sparkConf, clientArguments)

    val (host, port) = ArmadaUtils.parseMasterUrl(sparkConf.get("spark.master"))
    log(s"gbj2: Connecting to Armada Server - host: $host, port: $port")

    val armadaClient = ArmadaClient(host, port, useSsl = false, sparkConf.get(ARMADA_AUTH_TOKEN))
    val healthTimeout =
      Duration(sparkConf.get(ARMADA_HEALTH_CHECK_TIMEOUT), SECONDS)

    log(s"Checking Armada Server health (timeout: $healthTimeout)")
    val healthResp = Await.result(armadaClient.submitHealth(), healthTimeout)

    if (healthResp.status.isServing) {
      log("Armada Server is serving requests!")
    } else {
      throw new RuntimeException(
        "Armada health check failed - Armada Server is not serving requests!"
      )
    }

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
    // Disable ConfigMap creation as we do not have support for them in Armada
    conf.set("spark.kubernetes.executor.disableConfigMap", "true")

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
      .getOrElse(getApplicationId(conf))

    // Get basic feature steps from Spark's Kubernetes integration
    val (driverJobItem, driverContainer)     = getDriverFeatureSteps(conf, clientArguments)
    val (executorJobItem, executorContainer) = getExecutorFeatureSteps(conf, clientArguments)

    ArmadaJobConfig(
      queue = finalQueue,
      jobSetId = finalJobSetId,
      jobTemplate = jobTemplate,
      driverJobItemTemplate = driverJobItemTemplate,
      executorJobItemTemplate = executorJobItemTemplate,
      cliConfig = cliConfig,
      applicationId = getApplicationId(conf),
      driverFeatureStepJobItem = driverJobItem,
      driverFeatureStepContainer = driverContainer,
      executorFeatureStepJobItem = executorJobItem,
      executorFeatureStepContainer = executorContainer
    )
  }

  private case class DriverJobItemResult(
      jobItem: api.submit.JobSubmitRequestItem,
      configGenerator: ConfigGenerator,
      templateAnnotations: Map[String, String],
      templateLabels: Map[String, String],
      confSeq: Seq[String]
  )

  private def buildDriverJobItem(
      clientArguments: ClientArguments,
      armadaJobConfig: ArmadaJobConfig,
      conf: SparkConf,
      executorCount: Int,
      maybeGangId: Option[String]
  ): DriverJobItemResult = {
    val primaryResource = extractPrimaryResource(clientArguments.mainAppResource)
    val confSeq         = buildSparkConfArgs(conf)
    val configGenerator = new ConfigGenerator("armada-spark-config", conf)

    val (templateAnnotations, templateLabels) = extractTemplateMetadata(armadaJobConfig.jobTemplate)

    val runtimeAnnotations = buildAnnotations(
      configGenerator,
      templateAnnotations,
      armadaJobConfig.cliConfig.nodeUniformityLabel,
      conf
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

    val jobItem = createDriverJob(
      armadaJobConfig,
      resolvedConfig,
      configGenerator,
      clientArguments,
      primaryResource,
      confSeq,
      conf
    )

    DriverJobItemResult(
      jobItem = jobItem,
      configGenerator = configGenerator,
      templateAnnotations = templateAnnotations,
      templateLabels = templateLabels,
      confSeq = confSeq
    )
  }

  private[submit] def submitDriverJob(
      armadaClient: ArmadaClient,
      clientArguments: ClientArguments,
      armadaJobConfig: ArmadaJobConfig,
      conf: SparkConf
  ): String = {
    val executorCount = ModeHelper(conf).getExecutorCount
    val maybeGangId =
      armadaJobConfig.cliConfig.nodeUniformityLabel.map(_ => java.util.UUID.randomUUID.toString)

    val result = buildDriverJobItem(
      clientArguments,
      armadaJobConfig,
      conf,
      executorCount,
      maybeGangId
    )
    submitDriver(armadaClient, armadaJobConfig.queue, armadaJobConfig.jobSetId, result.jobItem)
  }

  private[spark] def submitExecutorJobs(
      armadaClient: ArmadaClient,
      clientArguments: ClientArguments,
      armadaJobConfig: ArmadaJobConfig,
      conf: SparkConf,
      driverJobId: String,
      executorCount: Int
  ): Seq[String] = {
    if (executorCount <= 0) {
      throw new IllegalArgumentException(
        s"Executor count must be greater than 0, but got: $executorCount"
      )
    }

    val driverResult = buildDriverJobItem(
      clientArguments,
      armadaJobConfig,
      conf,
      executorCount,
      None
    )

    val executorLabels = buildLabels(
      armadaJobConfig.cliConfig.podLabels,
      driverResult.templateLabels,
      armadaJobConfig.cliConfig.executorLabels
    )

    val executorRuntimeAnnotations = buildAnnotations(
      driverResult.configGenerator,
      driverResult.templateAnnotations,
      armadaJobConfig.cliConfig.nodeUniformityLabel,
      conf
    )

    val executorResolvedConfig = resolveJobConfig(
      armadaJobConfig.cliConfig,
      armadaJobConfig.executorJobItemTemplate,
      executorRuntimeAnnotations,
      executorLabels,
      conf
    )

    val driverHostname = ArmadaUtils.buildServiceNameFromJobId(driverJobId)
    val executors = createExecutorJobs(
      armadaJobConfig,
      executorResolvedConfig,
      driverResult.configGenerator,
      driverHostname,
      executorCount,
      conf
    )
    submitExecutors(
      armadaClient,
      armadaJobConfig.queue,
      armadaJobConfig.jobSetId,
      executors
    )

  }

  private[submit] def submitArmadaJob(
      armadaClient: ArmadaClient,
      clientArguments: ClientArguments,
      armadaJobConfig: ArmadaJobConfig,
      conf: SparkConf
  ): (String, Seq[String]) = {
    val executorCount = ModeHelper(conf).getExecutorCount
    if (executorCount <= 0) {
      throw new IllegalArgumentException(
        s"Executor count must be greater than 0, but got: $executorCount"
      )
    }
    val driverJobId = submitDriverJob(armadaClient, clientArguments, armadaJobConfig, conf)
    val executorJobIds = submitExecutorJobs(
      armadaClient,
      clientArguments,
      armadaJobConfig,
      conf,
      driverJobId,
      executorCount
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
      executorResources: ResourceConfig,
      driverIngress: Option[IngressConfig] = None
  )

  private[spark] case class ResourceConfig(
      limitCores: Option[String],
      requestCores: Option[String],
      limitMemory: Option[String],
      requestMemory: Option[String]
  )

  private[spark] case class IngressConfig(
      annotations: Map[String, String],
      tls: Option[Boolean],
      certName: Option[String]
  )

  private[spark] def parseCLIConfig(conf: SparkConf): CLIConfig = {
    // Extract CLI values only - validation handles defaults later
    val queue          = conf.get(ARMADA_JOB_QUEUE)
    val jobSetId       = conf.get(ARMADA_JOB_SET_ID)
    val runAsUser      = conf.get(ARMADA_RUN_AS_USER)
    val containerImage = conf.get(CONTAINER_IMAGE)
    containerImage.foreach { image =>
      conf.set(KUBERNETES_CONTAINER_IMAGE.key, image)
    }

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

    val driverIngress = if (conf.get(ARMADA_SPARK_DRIVER_INGRESS_ENABLED)) {
      Some(
        IngressConfig(
          annotations = conf
            .get(ARMADA_SPARK_DRIVER_INGRESS_ANNOTATIONS)
            .map(commaSeparatedAnnotationsToMap)
            .getOrElse(Map.empty),
          tls = conf.get(ARMADA_SPARK_DRIVER_INGRESS_TLS_ENABLED),
          certName = conf.get(ARMADA_SPARK_DRIVER_INGRESS_CERT_NAME)
        )
      )
    } else {
      None
    }

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
      executorResources = executorResources,
      driverIngress = driverIngress
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

    if (executorJobItemTemplate.exists(_.ingress.nonEmpty)) {
      throw new IllegalArgumentException(
        "Executor job item template must not contain ingress definition."
      )
    }

    if (driverJobItemTemplate.exists(_.ingress.size > 1)) {
      throw new IllegalArgumentException(
        "Driver job item template can contain only 1 ingress definition."
      )
    }

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
      executorResources: ResolvedResourceConfig,
      driverIngress: Option[api.submit.IngressConfig] = None
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

    val resolvedIngressConfig =
      if (cliConfig.driverIngress.isDefined || template.flatMap(_.ingress.headOption).isDefined) {
        Some(
          resolveIngressConfig(
            cliConfig.driverIngress,
            template.flatMap(_.ingress.headOption),
            conf
          )
        )
      } else {
        None
      }

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
      ),
      driverIngress = resolvedIngressConfig
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
    * @param driverFeatureStepPodSpec
    *   PodSpec from basic driver feature steps
    * @param driverFeatureStepContainer
    *   Container from basic driver feature steps
    * @param executorFeatureStepPodSpec
    *   PodSpec from basic executor feature steps
    * @param executorFeatureStepContainer
    *   Container from basic executor feature steps
    */
  private[spark] case class ArmadaJobConfig(
      queue: String,
      jobSetId: String,
      jobTemplate: Option[api.submit.JobSubmitRequest],
      driverJobItemTemplate: Option[api.submit.JobSubmitRequestItem],
      executorJobItemTemplate: Option[api.submit.JobSubmitRequestItem],
      cliConfig: CLIConfig,
      applicationId: String,
      driverFeatureStepJobItem: Option[api.submit.JobSubmitRequestItem],
      driverFeatureStepContainer: Option[Container],
      executorFeatureStepJobItem: Option[api.submit.JobSubmitRequestItem],
      executorFeatureStepContainer: Option[Container]
  )

  private def removeAuthToken(seq: Seq[String]): Seq[String] = {
    seq.grouped(2).toSeq.filter(!_(1).contains("auth.token")).flatten
  }

  private[submit] def createDriverJob(
      armadaJobConfig: ArmadaJobConfig,
      resolvedConfig: ResolvedJobConfig,
      configGenerator: ConfigGenerator,
      clientArguments: ClientArguments,
      primaryResource: Seq[String],
      confSeq: Seq[String],
      conf: SparkConf
  ): api.submit.JobSubmitRequestItem = {
    val driverArgs = removeAuthToken(confSeq) ++ primaryResource ++ clientArguments.driverArgs

    val driverJobItem = mergeDriverTemplate(
      armadaJobConfig.driverJobItemTemplate,
      resolvedConfig,
      armadaJobConfig,
      ArmadaClientApplication.DRIVER_PORT,
      clientArguments.mainClass,
      configGenerator.getVolumes,
      configGenerator.getVolumeMounts,
      driverArgs,
      conf
    )

    driverJobItem
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
      val executorJobItem = mergeExecutorTemplate(
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

      executorJobItem
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
    val error = Some(driverResponse.jobResponseItems.head.error)
      .filter(_.nonEmpty)
      .getOrElse("none")
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
      val error = Some(item.error).filter(_.nonEmpty).getOrElse("none")
      log(s"Submitted executor job with ID: ${item.jobId}, Error: $error")
      item.jobId
    }
  }

  /** Merges a driver job item template with runtime configuration.
    *
    * Merge order (later overrides earlier):
    *   1. Feature Steps (base from Spark)
    *   2. Template (user-provided YAML)
    *   3. CLI config (command-line flags)
    *   4. Armada requirements (hardcoded, always wins)
    */
  private[submit] def mergeDriverTemplate(
      template: Option[api.submit.JobSubmitRequestItem],
      resolvedConfig: ResolvedJobConfig,
      armadaJobConfig: ArmadaJobConfig,
      driverPort: Int,
      mainClass: String,
      volumes: Seq[Volume],
      volumeMounts: Seq[VolumeMount],
      additionalDriverArgs: Seq[String],
      conf: SparkConf
  ): api.submit.JobSubmitRequestItem = {

    val baseJobItem = armadaJobConfig.driverFeatureStepJobItem.getOrElse(createBlankTemplate())
    val basePodSpec = baseJobItem.podSpec.getOrElse(PodSpec())

    val afterTemplate = template.flatMap(_.podSpec) match {
      case Some(templatePodSpec) =>
        PodMerger.mergePodSpecs(base = basePodSpec, overriding = templatePodSpec)
      case None =>
        basePodSpec
    }
    val afterTemplatePod = PodSpecConverter.protobufPodSpecToFabric8Pod(afterTemplate)

    import scala.jdk.CollectionConverters._
    val afterCLIVolumes = if (volumes.nonEmpty) {
      val currentSpec =
        Option(afterTemplatePod.getSpec).getOrElse(new io.fabric8.kubernetes.api.model.PodSpec())
      val currentVolumes = Option(currentSpec.getVolumes)
        .map(_.asScala.toSeq)
        .getOrElse(Seq.empty)

      val fabric8CLIVolumes = volumes.flatMap(PodSpecConverter.convertVolumeToFabric8)

      val mergedVolumes = PodMerger.mergeByName(
        currentVolumes,
        fabric8CLIVolumes
      )(v => Option(v.getName))

      new io.fabric8.kubernetes.api.model.PodBuilder(afterTemplatePod)
        .editOrNewSpec()
        .withVolumes(mergedVolumes.asJava)
        .endSpec()
        .build()
    } else {
      afterTemplatePod
    }

    val templateVolumeMounts = Option(afterCLIVolumes.getSpec)
      .flatMap(spec => Option(spec.getContainers))
      .map(_.asScala.toSeq)
      .getOrElse(Seq.empty)
      .flatMap { c =>
        Option(c.getVolumeMounts).map(_.asScala.toSeq).getOrElse(Seq.empty)
      }
      .flatMap { vm =>
        PodSpecConverter.convertVolumeMount(vm)
      }

    val mergedVolumeMounts = PodMerger.mergeByName(
      templateVolumeMounts,
      volumeMounts
    )(_.name)

    val driverContainer = newDriverContainer(
      resolvedConfig.armadaClusterUrl,
      driverPort,
      mainClass,
      mergedVolumeMounts,
      additionalDriverArgs,
      armadaJobConfig,
      conf
    )

    val sidecars = extractSidecarContainers(baseJobItem.podSpec)

    val currentPodSpec    = PodSpecConverter.fabric8PodToProtobufPodSpec(afterCLIVolumes)
    val allInitContainers = currentPodSpec.initContainers

    // Set termination grace period for graceful decommissioning
    val gracePeriodSeconds = 0

    val finalPodSpec = currentPodSpec
      .withRestartPolicy("Never")
      .withTerminationGracePeriodSeconds(gracePeriodSeconds)
      .withContainers(Seq(driverContainer) ++ sidecars)
      .withInitContainers(allInitContainers)
      .withSecurityContext(new PodSecurityContext().withRunAsUser(resolvedConfig.runAsUser))
      .withNodeSelector(
        if (resolvedConfig.nodeSelectors.nonEmpty) resolvedConfig.nodeSelectors
        else currentPodSpec.nodeSelector
      )

    val services = buildServiceConfig(driverPort, conf)

    val finalJobItem = JobSubmitRequestItem(
      priority = if (resolvedConfig.priority != ArmadaClientApplication.DEFAULT_PRIORITY) {
        resolvedConfig.priority
      } else {
        template.map(_.priority).filter(_ != 0.0).getOrElse(baseJobItem.priority)
      },
      namespace = if (resolvedConfig.namespace != ArmadaClientApplication.DEFAULT_NAMESPACE) {
        resolvedConfig.namespace
      } else {
        template
          .map(_.namespace)
          .filter(_.nonEmpty)
          .orElse(Option(baseJobItem.namespace).filter(_.nonEmpty))
          .getOrElse(ArmadaClientApplication.DEFAULT_NAMESPACE)
      },
      labels =
        baseJobItem.labels ++ template.map(_.labels).getOrElse(Map.empty) ++ resolvedConfig.labels,
      annotations = baseJobItem.annotations ++ template
        .map(_.annotations)
        .getOrElse(Map.empty) ++ resolvedConfig.annotations,
      podSpec = Some(finalPodSpec),
      services = services
    )

    resolvedConfig.driverIngress match {
      case Some(ingress) => finalJobItem.withIngress(Seq(ingress))
      case None          => finalJobItem
    }
  }

  private def createBlankTemplate(): JobSubmitRequestItem = {
    api.submit
      .JobSubmitRequestItem()
      .withPriority(ArmadaClientApplication.DEFAULT_PRIORITY)
      .withNamespace(ArmadaClientApplication.DEFAULT_NAMESPACE)
      .withLabels(Map.empty)
      .withAnnotations(Map.empty)
      .withPodSpec(PodSpec().withNodeSelector(Map.empty))
  }

  /** Converts a fabric8 Kubernetes Pod to an Armada JobSubmitRequestItem.
    *
    * Extracts metadata (labels, annotations) and converts the PodSpec to protobuf format.
    */
  private def fabric8PodToJobItem(
      fabric8Pod: io.fabric8.kubernetes.api.model.Pod
  ): JobSubmitRequestItem = {
    val labels = Option(fabric8Pod.getMetadata)
      .flatMap(m => Option(m.getLabels))
      .map(_.asScala.toMap)
      .getOrElse(Map.empty)
    val annotations = Option(fabric8Pod.getMetadata)
      .flatMap(m => Option(m.getAnnotations))
      .map(_.asScala.toMap)
      .getOrElse(Map.empty)

    val podSpec = PodSpecConverter.fabric8ToProtobuf(fabric8Pod.getSpec)

    api.submit
      .JobSubmitRequestItem()
      .withLabels(labels)
      .withAnnotations(annotations)
      .withPodSpec(podSpec)
  }

  /** Extract non-Spark containers (sidecars) from a PodSpec. Filters out the main Spark
    * driver/executor container added by buildFromFeatures(). Spark's basic feature steps use
    * reserved names "driver" and "executor" for main containers. All other containers are
    * considered sidecars and preserved.
    */
  private def extractSidecarContainers(podSpec: Option[PodSpec]): Seq[Container] = {
    podSpec
      .map(_.containers)
      .getOrElse(Seq.empty)
      .filterNot(c =>
        c.name.exists { name =>
          val lowerName = name.toLowerCase
          lowerName == "driver" || lowerName == "executor"
        }
      )
  }

  /** Apply basic Kubernetes feature steps from Spark's KubernetesDriverBuilder.
    *
    * This method calls buildFromFeatures() to apply Spark's standard driver feature steps
    * (credentials, volumes, secrets, env vars, etc.) which are needed for proper Kubernetes
    * integration.
    *
    * @param conf
    *   Spark configuration
    * @param clientArguments
    *   Client arguments with application details
    * @return
    *   A tuple of (Some(JobSubmitRequestItem), Some(Container)) with basic feature steps applied.
    *   JobSubmitRequestItem contains labels, annotations, and PodSpec with init
    *   containers/sidecars. Container is the main Spark driver container with env vars and volume
    *   mounts.
    */
  private[spark] def getDriverFeatureSteps(
      conf: SparkConf,
      clientArguments: ClientArguments
  ): (Option[JobSubmitRequestItem], Option[Container]) = {
    if (conf.get("spark.driver.bindAddress", "").nonEmpty) {
      return (None, None)
    }
    val appId =
      conf.getOption("spark.app.id").getOrElse(ArmadaClientApplication.DEFAULT_ARMADA_APP_ID)

    // Clone conf to prevent feature step builders from mutating the original
    val driverSpec = new KubernetesDriverBuilder().buildFromFeatures(
      new KubernetesDriverConf(
        sparkConf = conf.clone(),
        appId = appId,
        mainAppResource = clientArguments.mainAppResource,
        mainClass = clientArguments.mainClass,
        appArgs = clientArguments.driverArgs,
        proxyUser = clientArguments.proxyUser
      ),
      new DefaultKubernetesClient()
    )

    val jobItem   = fabric8PodToJobItem(driverSpec.pod.pod)
    val container = PodSpecConverter.convertContainer(driverSpec.pod.container)

    (Some(jobItem), Some(container))
  }

  /** Apply basic Kubernetes feature steps from Spark's KubernetesExecutorBuilder.
    *
    * This method calls buildFromFeatures() to apply Spark's standard executor feature steps
    * (credentials, volumes, secrets, env vars, etc.) which are needed for proper Kubernetes
    * integration.
    *
    * @param conf
    *   Spark configuration
    * @param clientArguments
    *   Client arguments with application details
    * @return
    *   A tuple of (Some(JobSubmitRequestItem), Some(Container)) with basic feature steps applied.
    *   JobSubmitRequestItem contains labels, annotations, and PodSpec with init
    *   containers/sidecars. Container is the main Spark executor container with env vars and volume
    *   mounts.
    */
  private[spark] def getExecutorFeatureSteps(
      conf: SparkConf,
      clientArguments: ClientArguments
  ): (Option[JobSubmitRequestItem], Option[Container]) = {
    val appId =
      conf.getOption("spark.app.id").getOrElse(ArmadaClientApplication.DEFAULT_ARMADA_APP_ID)

    // Clone conf to prevent feature step builders from mutating the original
    val clonedConf = conf.clone()
    val executorConf = new KubernetesExecutorConf(
      sparkConf = clonedConf,
      appId = appId,
      executorId = "0",
      driverPod = None
    )

    val executorSpec = new KubernetesExecutorBuilder().buildFromFeatures(
      executorConf,
      new SecurityManager(clonedConf),
      new DefaultKubernetesClient(),
      ResourceProfile.getOrCreateDefaultProfile(clonedConf)
    )

    val jobItem   = fabric8PodToJobItem(executorSpec.pod.pod)
    val container = PodSpecConverter.convertContainer(executorSpec.pod.container)

    (Some(jobItem), Some(container))
  }

  /** Merges an executor job item template with runtime configuration.
    *
    * Merge order (later overrides earlier):
    *   1. Feature Steps (base from Spark)
    *   2. Template (user-provided YAML)
    *   3. CLI config (command-line flags)
    *   4. Armada requirements (hardcoded, always wins)
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

    val baseJobItem = armadaJobConfig.executorFeatureStepJobItem.getOrElse(createBlankTemplate())
    val basePodSpec = baseJobItem.podSpec.getOrElse(PodSpec())

    val afterTemplate = template.flatMap(_.podSpec) match {
      case Some(templatePodSpec) =>
        PodMerger.mergePodSpecs(base = basePodSpec, overriding = templatePodSpec)
      case None =>
        basePodSpec
    }
    val afterTemplatePod = PodSpecConverter.protobufPodSpecToFabric8Pod(afterTemplate)

    import scala.jdk.CollectionConverters._
    val afterCLIVolumes = if (volumes.nonEmpty) {
      val currentSpec =
        Option(afterTemplatePod.getSpec).getOrElse(new io.fabric8.kubernetes.api.model.PodSpec())
      val currentVolumes = Option(currentSpec.getVolumes)
        .map(_.asScala.toSeq)
        .getOrElse(Seq.empty)

      val fabric8CLIVolumes = volumes.flatMap(PodSpecConverter.convertVolumeToFabric8)

      val mergedVolumes = PodMerger.mergeByName(
        currentVolumes,
        fabric8CLIVolumes
      )(v => Option(v.getName))

      new io.fabric8.kubernetes.api.model.PodBuilder(afterTemplatePod)
        .editOrNewSpec()
        .withVolumes(mergedVolumes.asJava)
        .endSpec()
        .build()
    } else {
      afterTemplatePod
    }

    val featureStepVolumeMounts = armadaJobConfig.executorFeatureStepContainer
      .map(_.volumeMounts)
      .getOrElse(Seq.empty)

    val templateVolumeMounts = Option(afterCLIVolumes.getSpec)
      .flatMap(spec => Option(spec.getContainers))
      .map(_.asScala.toSeq)
      .getOrElse(Seq.empty)
      .flatMap { c =>
        Option(c.getVolumeMounts).map(_.asScala.toSeq).getOrElse(Seq.empty)
      }
      .flatMap { vm =>
        PodSpecConverter.convertVolumeMount(vm)
      }

    val mergedVolumeMounts = PodMerger.mergeByName(
      featureStepVolumeMounts,
      templateVolumeMounts
    )(_.name)

    val executorContainer = newExecutorContainer(
      index,
      driverHostname,
      driverPort,
      armadaJobConfig.cliConfig.nodeUniformityLabel,
      javaOptEnvVars,
      armadaJobConfig,
      conf
    ).withVolumeMounts(mergedVolumeMounts)

    val sidecars = extractSidecarContainers(baseJobItem.podSpec)

    val executorInitContainer = newExecutorInitContainer(
      driverHostname,
      driverPort,
      resolvedConfig.executorConnectionTimeout,
      conf.get(ARMADA_EXECUTOR_INIT_CONTAINER_IMAGE),
      conf.get(ARMADA_EXECUTOR_INIT_CONTAINER_CPU),
      conf.get(ARMADA_EXECUTOR_INIT_CONTAINER_MEMORY)
    )

    val currentPodSpec = PodSpecConverter.fabric8PodToProtobufPodSpec(afterCLIVolumes)
    val allInitContainers =
      PodMerger.mergeByName(currentPodSpec.initContainers, Seq(executorInitContainer))(_.name)

    // Set termination grace period for graceful decommissioning
    val gracePeriodSeconds = 0

    val finalPodSpec = currentPodSpec
      .withRestartPolicy("Never")
      .withTerminationGracePeriodSeconds(gracePeriodSeconds)
      .withContainers(Seq(executorContainer) ++ sidecars)
      .withInitContainers(allInitContainers)
      .withSecurityContext(new PodSecurityContext().withRunAsUser(resolvedConfig.runAsUser))
      .withNodeSelector(
        if (resolvedConfig.nodeSelectors.nonEmpty) resolvedConfig.nodeSelectors
        else currentPodSpec.nodeSelector
      )

    JobSubmitRequestItem(
      priority = if (resolvedConfig.priority != ArmadaClientApplication.DEFAULT_PRIORITY) {
        resolvedConfig.priority
      } else {
        template.map(_.priority).filter(_ != 0.0).getOrElse(baseJobItem.priority)
      },
      namespace = if (resolvedConfig.namespace != ArmadaClientApplication.DEFAULT_NAMESPACE) {
        resolvedConfig.namespace
      } else {
        template
          .map(_.namespace)
          .filter(_.nonEmpty)
          .orElse(Option(baseJobItem.namespace).filter(_.nonEmpty))
          .getOrElse(ArmadaClientApplication.DEFAULT_NAMESPACE)
      },
      labels =
        baseJobItem.labels ++ template.map(_.labels).getOrElse(Map.empty) ++ resolvedConfig.labels,
      annotations = baseJobItem.annotations ++ template
        .map(_.annotations)
        .getOrElse(Map.empty) ++ resolvedConfig.annotations,
      podSpec = Some(finalPodSpec)
    )
  }

  private def newDriverContainer(
      master: String,
      port: Int,
      mainClass: String,
      volumeMounts: Seq[VolumeMount],
      additionalDriverArgs: Seq[String],
      armadaJobConfig: ArmadaJobConfig,
      conf: SparkConf
  ): Container = {
    val source = EnvVarSource().withFieldRef(
      ObjectFieldSelector()
        .withApiVersion("v1")
        .withFieldPath("status.podIP")
    )
    val armadaJobIdSource = EnvVarSource().withFieldRef(
      ObjectFieldSelector()
        .withApiVersion("v1")
        .withFieldPath("metadata.labels['armada_job_id']")
    )
    val armadaJobSetIdSource = EnvVarSource().withFieldRef(
      ObjectFieldSelector()
        .withApiVersion("v1")
        .withFieldPath("metadata.annotations['armada_jobset_id']")
    )

    val newEnvVars = Seq(
      EnvVar().withName("SPARK_DRIVER_BIND_ADDRESS").withValueFrom(source),
      EnvVar()
        .withName(ConfigGenerator.ENV_SPARK_CONF_DIR)
        .withValue(ConfigGenerator.REMOTE_CONF_DIR_NAME),
      EnvVar().withName("ARMADA_JOB_ID").withValueFrom(armadaJobIdSource),
      EnvVar().withName("ARMADA_JOB_SET_ID").withValueFrom(armadaJobSetIdSource)
    )

    val featureStepEnvVars = armadaJobConfig.driverFeatureStepContainer
      .map(_.env)
      .getOrElse(Seq.empty)
    val envVars = featureStepEnvVars ++ newEnvVars

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

    val containerImage = armadaJobConfig.cliConfig.containerImage
      .orElse(extractContainerImageFromTemplate(armadaJobConfig.driverJobItemTemplate))
      .get // Safe to use .get because validation ensures container image exists
    Container()
      .withName("driver")
      .withImage(containerImage)
      .withImagePullPolicy("IfNotPresent")
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
          s"spark.app.id=${armadaJobConfig.applicationId}",
          "--conf",
          "spark.driver.host=$(SPARK_DRIVER_BIND_ADDRESS)"
        ) ++ additionalDriverArgs
      )
      .withVolumeMounts(
        armadaJobConfig.driverFeatureStepContainer
          .map(_.volumeMounts)
          .getOrElse(Seq.empty) ++ volumeMounts
      )
      .withEnv(envVars)
      .withResources(
        ResourceRequirements(
          requests = driverRequests,
          limits = driverLimits
        )
      )
      .withPorts({
        Seq(
          ContainerPort(
            containerPort = Some(port),
            name = Some("driver"),
            protocol = Some("TCP")
          ),
          ContainerPort(
            containerPort = Some(conf.getOption("spark.ui.port").map(_.toInt).getOrElse(4040)),
            name = Some("ui"),
            protocol = Some("TCP")
          )
        )
      })
  }

  private def newExecutorContainer(
      index: Int,
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
    val podName = EnvVarSource().withFieldRef(
      ObjectFieldSelector()
        .withApiVersion("v1")
        .withFieldPath("metadata.name")
    )
    val armadaJobIdSource = EnvVarSource().withFieldRef(
      ObjectFieldSelector()
        .withApiVersion("v1")
        .withFieldPath("metadata.labels['armada_job_id']")
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

    val newEnvVars = Seq(
      EnvVar().withName("SPARK_EXECUTOR_ID").withValue(index.toString),
      EnvVar().withName("SPARK_RESOURCE_PROFILE_ID").withValue("0"),
      // Ensure executor pod name is based on Armada job id label by referencing ARMADA_JOB_ID
      EnvVar().withName("SPARK_EXECUTOR_POD_NAME").withValueFrom(armadaJobIdSource),
      EnvVar().withName("ARMADA_JOB_ID").withValueFrom(armadaJobIdSource),
      EnvVar()
        .withName("SPARK_APPLICATION_ID")
        .withValue(armadaJobConfig.applicationId),
      EnvVar().withName("SPARK_EXECUTOR_CORES").withValue(sparkExecutorCores),
      EnvVar().withName("SPARK_EXECUTOR_MEMORY").withValue(sparkExecutorMemory),
      EnvVar().withName("SPARK_DRIVER_URL").withValue(driverURL),
      EnvVar().withName("SPARK_EXECUTOR_POD_IP").withValueFrom(source)
    ) ++ nodeUniformityLabel
      .map(label => EnvVar().withName("ARMADA_SPARK_GANG_NODE_UNIFORMITY_LABEL").withValue(label))

    val featureStepEnvVars = armadaJobConfig.executorFeatureStepContainer
      .map(_.env)
      .getOrElse(Seq.empty)
    val envVars = featureStepEnvVars ++ newEnvVars

    val containerImage = armadaJobConfig.cliConfig.containerImage
      .orElse(extractContainerImageFromTemplate(armadaJobConfig.executorJobItemTemplate))
      .get // Safe to use .get because validation ensures container image exists
    Container()
      .withName("executor")
      .withImage(containerImage)
      .withImagePullPolicy("IfNotPresent")
      .withArgs(
        Seq(
          "executor",
          "--cores",
          sparkExecutorCores,
          "--app-id",
          armadaJobConfig.applicationId,
          "--hostname",
          "$(SPARK_EXECUTOR_POD_IP)"
        )
      )
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
      connectionTimeout: Duration,
      image: String,
      cpu: String,
      memory: String
  ) = {
    val initContainerResources = ResourceRequirements(
      requests = Map(
        "cpu"    -> Quantity(Option(cpu)),
        "memory" -> Quantity(Option(memory))
      ),
      limits = Map(
        "cpu"    -> Quantity(Option(cpu)),
        "memory" -> Quantity(Option(memory))
      )
    )

    Container()
      .withName("wait-for-driver")
      .withImagePullPolicy("IfNotPresent")
      .withImage(image)
      .withResources(initContainerResources)
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
      .withResources(initContainerResources)
  }

  // Resolves a value based on precedence: CLI > Template > Default
  private[submit] def resolveValue[T](
      cliValue: Option[T],
      templateValue: => Option[T],
      defaultValue: => T
  ): T = {
    cliValue.orElse(templateValue).getOrElse(defaultValue)
  }

  // Service ordering: driver port first so executors can connect to service-0
  private[submit] def buildServiceConfig(
      driverPort: Int,
      conf: SparkConf
  ): Seq[api.submit.ServiceConfig] = {
    val requiredPorts = scala.collection.mutable.LinkedHashSet(driverPort)

    val uiPort = conf.getOption("spark.ui.port").map(_.toInt).getOrElse(4040)
    requiredPorts += uiPort

    Seq(
      api.submit.ServiceConfig(
        `type` = api.submit.ServiceType.Headless,
        ports = requiredPorts.toSeq,
        name = ""
      )
    )
  }

  private[submit] def resolveIngressConfig(
      cliIngress: Option[IngressConfig],
      templateIngress: Option[api.submit.IngressConfig],
      conf: SparkConf
  ): api.submit.IngressConfig = {
    val mergedAnnotations = templateIngress
      .map(_.annotations)
      .getOrElse(Map.empty) ++
      cliIngress
        .map(_.annotations)
        .getOrElse(Map.empty)

    val uiPort = conf.getOption("spark.ui.port").map(_.toInt).getOrElse(4040)
    val ports  = Seq(uiPort)

    api.submit.IngressConfig(
      ports = ports,
      annotations = mergedAnnotations,
      tlsEnabled = resolveValue(
        cliIngress.flatMap(_.tls),
        templateIngress.map(_.tlsEnabled),
        false
      ),
      certName = resolveValue(
        cliIngress.flatMap(_.certName),
        templateIngress.map(_.certName),
        ""
      )
    )
  }

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

  // Convert the space-delimited "spark.executor.extraJavaOptions" into env vars that can be used by entrypoint.sh
  private def javaOptEnvVars(conf: SparkConf) = {
    // The executor's java opts are handled as env vars in the docker entrypoint.sh here:
    // https://github.com/apache/spark/blob/v3.5.3/resource-managers/kubernetes/docker/src/main/dockerfiles/spark/entrypoint.sh#L44-L46

    // entrypoint.sh then adds those to the jvm command line here:
    // https://github.com/apache/spark/blob/v3.5.3/resource-managers/kubernetes/docker/src/main/dockerfiles/spark/entrypoint.sh#L96
    val javaOpts =
      conf
        .getOption("spark.executor.extraJavaOptions")
        .map(_.split(" ").toSeq)
        .getOrElse(
          Seq()
        ) ++ "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/jdk.internal.ref=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED"
        .split(" ")
        .toSeq

    javaOpts.zipWithIndex.map { case (value: String, index) =>
      EnvVar().withName("SPARK_JAVA_OPT_" + index).withValue(value)
    }.toVector
  }

  /** Merges two sequences of environment variables, with values from secondSeq taking precedence
    * for duplicate names
    *
    * @param firstSeq
    *   First sequence of environment variables
    * @param secondSeq
    *   Second sequence of environment variables
    * @return
    *   Merged sequence with duplicates resolved from secondSeq
    */
  private[spark] def mergeEnvVars(firstSeq: Seq[EnvVar], secondSeq: Seq[EnvVar]): Seq[EnvVar] = {
    val index: EnvVar => (String, EnvVar) = env => env.getName -> env
    val merged                            = firstSeq.map(index).toMap ++ secondSeq.map(index).toMap
    merged.values.toSeq
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
      conf: SparkConf
  ): Map[String, String] = {
    val modeHelper = ModeHelper(conf)
    configGenerator.getAnnotations ++ templateAnnotations ++ nodeUniformityLabel
      .map(label =>
        GangSchedulingAnnotations(
          ArmadaClientApplication.gangId,
          modeHelper.getGangCardinality,
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
