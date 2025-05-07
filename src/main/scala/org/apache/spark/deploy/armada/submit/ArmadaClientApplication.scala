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

/*
import scala.jdk.CollectionConverters._
import scala.util.control.Breaks._
import scala.util.control.NonFatal
*/

/*
import io.fabric8.kubernetes.api.model._
import io.fabric8.kubernetes.client.{KubernetesClient, Watch}
import io.fabric8.kubernetes.client.Watcher.Action
*/
import _root_.io.armadaproject.armada.ArmadaClient
import k8s.io.api.core.v1.generated._
import k8s.io.apimachinery.pkg.api.resource.generated.Quantity
import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.deploy.SparkApplication
import org.apache.spark.deploy.armada.Config._
import org.apache.spark.deploy.armada.submit.ArmadaClientApplication._
import org.apache.spark.deploy.armada.submit.GangSchedulingAnnotations._
import org.apache.spark.scheduler.cluster.SchedulerBackendUtils

import java.util.UUID

/* import org.apache.spark.deploy.k8s._
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.KubernetesUtils.addOwnerReference
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys.{APP_ID, APP_NAME, SUBMISSION_ID}
import org.apache.spark.util.Utils
*/

/**
 * Encapsulates arguments to the submission client.
 *
 * @param mainAppResource the main application resource if any
 * @param mainClass the main class of the application to run
 * @param driverArgs arguments to the driver
 */
private[spark] case class ClientArguments(
    mainAppResource: MainAppResource,
    mainClass: String,
    driverArgs: Array[String],
    proxyUser: Option[String])

private[spark] object ClientArguments {

  def fromCommandLineArgs(args: Array[String]): ClientArguments = {
    var mainAppResource: MainAppResource = JavaMainAppResource(None)
    var mainClass: Option[String] = None
    val driverArgs = mutable.ArrayBuffer.empty[String]
    var proxyUser: Option[String] = None

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

    require(mainClass.isDefined, "Main class must be specified via --main-class")

    ClientArguments(
      mainAppResource,
      mainClass.get,
      driverArgs.toArray,
      proxyUser)
  }
}

/**
 * Submits a Spark application to run on Kubernetes by creating the driver pod and starting a
 * watcher that monitors and logs the application status. Waits for the application to terminate if
 * spark.kubernetes.submission.waitAppCompletion is true.
 *
 * @param conf The kubernetes driver config.
 * @param builder Responsible for building the base driver pod based on a composition of
 *                implemented features.
 * @param kubernetesClient the client to talk to the Kubernetes API server
 * @param watcher a watcher that monitors and logs the application status
 */
/* FIXME: Have an Armada Client instead.
private[spark] class Client(
    conf: KubernetesDriverConf,
    builder: KubernetesDriverBuilder,
    kubernetesClient: KubernetesClient,
    watcher: LoggingPodStatusWatcher) extends Logging {

  def run(): Unit = {
    val resolvedDriverSpec = builder.buildFromFeatures(conf, kubernetesClient)
    val configMapName = KubernetesClientUtils.configMapNameDriver
    val confFilesMap = KubernetesClientUtils.buildSparkConfDirFilesMap(configMapName,
      conf.sparkConf, resolvedDriverSpec.systemProperties)
    val configMap = KubernetesClientUtils.buildConfigMap(configMapName, confFilesMap +
        (KUBERNETES_NAMESPACE.key -> conf.namespace))

    // The include of the ENV_VAR for "SPARK_CONF_DIR" is to allow for the
    // Spark command builder to pickup on the Java Options present in the ConfigMap
    val resolvedDriverContainer = new ContainerBuilder(resolvedDriverSpec.pod.container)
      .addNewEnv()
        .withName(ENV_SPARK_CONF_DIR)
        .withValue(SPARK_CONF_DIR_INTERNAL)
        .endEnv()
      .addNewVolumeMount()
        .withName(SPARK_CONF_VOLUME_DRIVER)
        .withMountPath(SPARK_CONF_DIR_INTERNAL)
        .endVolumeMount()
      .build()
    val resolvedDriverPod = new PodBuilder(resolvedDriverSpec.pod.pod)
      .editSpec()
        .addToContainers(resolvedDriverContainer)
        .addNewVolume()
          .withName(SPARK_CONF_VOLUME_DRIVER)
          .withNewConfigMap()
            .withItems(KubernetesClientUtils.buildKeyToPathObjects(confFilesMap).asJava)
            .withName(configMapName)
            .endConfigMap()
          .endVolume()
        .endSpec()
      .build()
    val driverPodName = resolvedDriverPod.getMetadata.getName

    // setup resources before pod creation
    val preKubernetesResources = resolvedDriverSpec.driverPreKubernetesResources
    try {
      kubernetesClient.resourceList(preKubernetesResources: _*).forceConflicts().serverSideApply()
    } catch {
      case NonFatal(e) =>
        logError("Please check \"kubectl auth can-i create [resource]\" first." +
          " It should be yes. And please also check your feature step implementation.")
        kubernetesClient.resourceList(preKubernetesResources: _*).delete()
        throw e
    }

    var watch: Watch = null
    var createdDriverPod: Pod = null
    try {
      createdDriverPod =
        kubernetesClient.pods().inNamespace(conf.namespace).resource(resolvedDriverPod).create()
    } catch {
      case NonFatal(e) =>
        kubernetesClient.resourceList(preKubernetesResources: _*).delete()
        logError("Please check \"kubectl auth can-i create pod\" first. It should be yes.")
        throw e
    }

    // Refresh all pre-resources' owner references
    try {
      addOwnerReference(createdDriverPod, preKubernetesResources)
      kubernetesClient.resourceList(preKubernetesResources: _*).forceConflicts().serverSideApply()
    } catch {
      case NonFatal(e) =>
        kubernetesClient.pods().resource(createdDriverPod).delete()
        kubernetesClient.resourceList(preKubernetesResources: _*).delete()
        throw e
    }

    // setup resources after pod creation, and refresh all resources' owner references
    try {
      val otherKubernetesResources = resolvedDriverSpec.driverKubernetesResources ++ Seq(configMap)
      addOwnerReference(createdDriverPod, otherKubernetesResources)
      kubernetesClient.resourceList(otherKubernetesResources: _*).forceConflicts().serverSideApply()
    } catch {
      case NonFatal(e) =>
        kubernetesClient.pods().resource(createdDriverPod).delete()
        throw e
    }

    val sId = Client.submissionId(conf.namespace, driverPodName)
    if (conf.get(WAIT_FOR_APP_COMPLETION)) {
      breakable {
        while (true) {
          val podWithName = kubernetesClient
            .pods()
            .inNamespace(conf.namespace)
            .withName(driverPodName)
          // Reset resource to old before we start the watch, this is important for race conditions
          watcher.reset()
          watch = podWithName.watch(watcher)

          // Send the latest pod state we know to the watcher to make sure we didn't miss anything
          watcher.eventReceived(Action.MODIFIED, podWithName.get())

          // Break the while loop if the pod is completed or we don't want to wait
          if (watcher.watchOrStop(sId)) {
            watch.close()
            break()
          }
        }
      }
    } else {
      logInfo(log"Deployed Spark application ${MDC(APP_NAME, conf.appName)} with " +
        log"application ID ${MDC(APP_ID, conf.appId)} and " +
        log"submission ID ${MDC(SUBMISSION_ID, sId)} into Kubernetes")
    }
  }
}
*/

private[spark] object Client {
  def submissionId(namespace: String, driverPodName: String): String = s"$namespace:$driverPodName"
}

private[spark] object ArmadaClientApplication {
  private val PROFILE_ID = "0"
  private val DRIVER_CONTAINER_NAME = "armada-spark-driver"
  private val EXECUTOR_CONTAINER_NAME = "armada-spark-executor"
  private val DRIVER_PORT_NAME = "armada-spark"
  private val DRIVER_HOST_PORT = 0
  private val DRIVER_PORT = 7078
  private val EXECUTOR_ENTRYPOINT_ARG = "executor"
  private val DRIVER_ENTRYPOINT_ARG = "driver"
  private val ENTRYPOINT = "/opt/entrypoint.sh"
}

/**
 * Main class and entry point of application submission in KUBERNETES mode.
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

  private def run(clientArguments: ClientArguments, sparkConf: SparkConf): Unit = {
    val (host, port) = ArmadaUtils.parseMasterUrl(sparkConf.get("spark.master"))
    log(s"host is $host, port is $port")
    val armadaClient = ArmadaClient(host, port)
    val healthTimeout = Duration(sparkConf.get(ARMADA_HEALTH_CHECK_TIMEOUT), SECONDS)
    val healthResp = Await.result(armadaClient.submitHealth(), healthTimeout)

    if (healthResp.status.isServing) {
      log("Submit health good!")
    } else {
      log("Could not contact Armada!")
    }

    // # FIXME: Need to check how this is launched whether to submit a job or
    // to turn into driver / cluster manager mode.
    val jobId = submitJobs(armadaClient, clientArguments, sparkConf)

    val lookoutBaseURL = sparkConf.get(ARMADA_LOOKOUTURL)
    val lookoutURL = s"$lookoutBaseURL/?page=0&sort[id]=jobId&sort[desc]=true&" +
      s"ps=50&sb=$jobId&active=false&refresh=true"
    log(s"Lookout URL for this job is $lookoutURL")

    // For constructing the app ID, we can't use the Spark application name, as the app ID is going
    // to be added as a label to group resources belonging to the same application. Label values are
    // considerably restrictive, e.g. must be no longer than 63 characters in length. So we generate
    // a unique app ID (captured by spark.app.id) in the format below.
    /*
    val kubernetesAppId = KubernetesConf.getKubernetesAppId()
    val kubernetesConf = KubernetesConf.createDriverConf(
      sparkConf,
      kubernetesAppId,
      clientArguments.mainAppResource,
      clientArguments.mainClass,
      clientArguments.driverArgs,
      clientArguments.proxyUser)
    // The master URL has been checked for validity already in SparkSubmit.
    // We just need to get rid of the "k8s://" prefix here.
    val master = KubernetesUtils.parseMasterUrl(sparkConf.get("spark.master"))
    val watcher = new LoggingPodStatusWatcherImpl(kubernetesConf)

    Utils.tryWithResource(SparkKubernetesClientFactory.createKubernetesClient(
      master,
      Some(kubernetesConf.namespace),
      KUBERNETES_AUTH_SUBMISSION_CONF_PREFIX,
      SparkKubernetesClientFactory.ClientType.Submission,
      sparkConf,
      None)) { kubernetesClient =>
        val client = new Client(
          kubernetesConf,
          new KubernetesDriverBuilder(),
          kubernetesClient,
          watcher)
        client.run()
    }
    */
    ()
  }

  private def getExecutorContainers(numExecutors: Int, driverServiceName: String, conf: SparkConf): Seq[Container] = {
    (0 until numExecutors).map
      {getExecutorContainer(_, driverServiceName, conf)}
  }

  // Convert the space-delimited "spark.executor.extraJavaOptions" into env vars that can be used by entrypoint.sh
  private def javaOptEnvVars(conf: SparkConf) = {
    // The executor's java opts are handled as env vars in the docker entrypoint.sh here:
    // https://github.com/apache/spark/blob/v3.5.3/resource-managers/kubernetes/docker/src/main/dockerfiles/spark/entrypoint.sh#L44-L46

    // entrypoint.sh then adds those to the jvm command line here:
    // https://github.com/apache/spark/blob/v3.5.3/resource-managers/kubernetes/docker/src/main/dockerfiles/spark/entrypoint.sh#L96
    // TODO: this configuration option appears to not be set... is that a problem?
    val javaOpts =
      conf.getOption("spark.executor.extraJavaOptions").map(_.split(" ").toSeq)
        .getOrElse(Seq()) :+ "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"

    javaOpts.zipWithIndex.map {
      case(value: String, index) =>
        EnvVar().withName("SPARK_JAVA_OPT_" + index).withValue(value)
    }
  }

  private[spark] def getExecutorContainer(executorID: Int, driverServiceName: String, conf: SparkConf): Container = {
    val executorContainerImage = conf.get(EXECUTOR_CONTAINER_IMAGE)
      .getOrElse(throw new SparkException("Must specify the executor container image"))
    val driverURL = s"spark://CoarseGrainedScheduler@$driverServiceName:$DRIVER_PORT"
    val source = EnvVarSource().withFieldRef(ObjectFieldSelector()
      .withApiVersion("v1").withFieldPath("status.podIP"))
    val podName = EnvVarSource().withFieldRef(ObjectFieldSelector()
      .withApiVersion("v1").withFieldPath("metadata.name"))

    val sparkExecutorMemory = conf.getOption("spark.executor.memory").getOrElse(DEFAULT_SPARK_EXECUTOR_MEMORY)
    val sparkExecutorCores = conf.getOption("spark.executor.cores").getOrElse(DEFAULT_SPARK_EXECUTOR_CORES)

    val envVars = Seq(
      EnvVar().withName("SPARK_EXECUTOR_ID").withValue(executorID.toString),
      EnvVar().withName("SPARK_RESOURCE_PROFILE_ID").withValue(PROFILE_ID),
      EnvVar().withName("SPARK_EXECUTOR_POD_NAME").withValueFrom(podName),
      EnvVar().withName("SPARK_APPLICATION_ID").withValue(conf.getOption("spark.app.id").getOrElse(DEFAULT_ARMADA_APP_ID)),
      EnvVar().withName("SPARK_EXECUTOR_CORES").withValue(sparkExecutorCores),
      EnvVar().withName("SPARK_EXECUTOR_MEMORY").withValue(sparkExecutorMemory),
      EnvVar().withName("SPARK_DRIVER_URL").withValue(driverURL),
      EnvVar().withName("SPARK_EXECUTOR_POD_IP").withValueFrom(source),
      EnvVar().withName("ARMADA_SPARK_GANG_NODE_UNIFORMITY_LABEL")
        .withValue(conf.get(GANG_SCHEDULING_NODE_UNIFORMITY_LABEL))
    )
    Container()
      .withName(s"${EXECUTOR_CONTAINER_NAME}-$executorID")
      .withImagePullPolicy("IfNotPresent")
      .withImage(executorContainerImage)
      .withEnv(envVars ++ javaOptEnvVars(conf))
      .withCommand(Seq(ENTRYPOINT))
      .withArgs(
        Seq(
          EXECUTOR_ENTRYPOINT_ARG
        )
      )
      .withResources(
        ResourceRequirements(
          limits = Map(
            "memory" -> Quantity(Option(conf.get(ARMADA_EXECUTOR_LIMIT_MEMORY))),
            "ephemeral-storage" -> Quantity(Option(conf.get(ARMADA_EXECUTOR_LIMIT_EPHEMERAL_STORAGE))),
            "cpu" -> Quantity(Option(conf.get(ARMADA_EXECUTOR_LIMIT_CORES).toString))
          ),
          requests = Map(
            "memory" -> Quantity(Option(conf.get(ARMADA_EXECUTOR_REQUEST_MEMORY))),
            "ephemeral-storage" -> Quantity(Option(conf.get(ARMADA_EXECUTOR_REQUEST_EPHEMERAL_STORAGE))),
            "cpu" -> Quantity(Option(conf.get(ARMADA_EXECUTOR_REQUEST_CORES).toString))
          )
        )
      )
  }

  private[spark] def getDriverContainer(driverServiceName: String, clientArguments: ClientArguments,
      conf: SparkConf, volumeMounts: Seq[VolumeMount] ): Container = {
    val source = EnvVarSource().withFieldRef(ObjectFieldSelector()
      .withApiVersion("v1").withFieldPath("status.podIP"))
    val envVars = Seq(
      EnvVar().withName("SPARK_DRIVER_BIND_ADDRESS").withValueFrom(source),
      EnvVar().withName(ConfigGenerator.ENV_SPARK_CONF_DIR).withValue(ConfigGenerator.REMOTE_CONF_DIR_NAME),
      EnvVar().withName("EXTERNAL_CLUSTER_SUPPORT_ENABLED").withValue("true"),
      EnvVar().withName("ARMADA_SPARK_DRIVER_SERVICE_NAME").withValue(driverServiceName)
    )

    val primaryResource = clientArguments.mainAppResource match {
      case JavaMainAppResource(Some(resource)) => Seq(resource)
      case PythonMainAppResource(resource) => Seq(resource)
      case RMainAppResource(resource) => Seq(resource)
      case _ => Seq()
    }

    val confSeq = conf.getAll.flatMap {
      case(k, v) => Seq("--conf", s"$k=$v")
    }
    val driverContainerImage = conf.get(DRIVER_CONTAINER_IMAGE)
      .getOrElse(throw new SparkException("Must specify the driver container image"))


    Container().withName(DRIVER_CONTAINER_NAME)
      .withImagePullPolicy("IfNotPresent")
      .withImage(driverContainerImage)
      .withEnv(envVars)
      .withCommand(Seq(ENTRYPOINT))
      .withVolumeMounts(volumeMounts)
      .withPorts(Seq(ContainerPort(Option(DRIVER_PORT_NAME), Option(DRIVER_HOST_PORT), Option(DRIVER_PORT))))
      .withArgs(
        Seq(
          DRIVER_ENTRYPOINT_ARG,
          "--verbose",
          "--class",
          clientArguments.mainClass,
          "--master",
          s"${conf.get(ARMADA_REMOTE_MASTER).getOrElse(conf.get("spark.master"))}",
          "--conf",
          s"spark.driver.port=$DRIVER_PORT",
          "--conf",
          "spark.driver.host=$(SPARK_DRIVER_BIND_ADDRESS)"
        ) ++ confSeq ++ primaryResource ++ clientArguments.driverArgs
      )
      .withResources(
        ResourceRequirements(
          limits = Map(
            "memory" -> Quantity(Option(conf.get(ARMADA_DRIVER_LIMIT_MEMORY))),
            "ephemeral-storage" -> Quantity(Option(conf.get(ARMADA_DRIVER_LIMIT_EPHEMERAL_STORAGE))),
            "cpu" -> Quantity(Option(conf.get(ARMADA_DRIVER_LIMIT_CORES).toString))
          ),
          requests = Map(
            "memory" -> Quantity(Option(conf.get(ARMADA_DRIVER_REQUEST_MEMORY))),
            "ephemeral-storage" -> Quantity(Option(conf.get(ARMADA_DRIVER_REQUEST_EPHEMERAL_STORAGE))),
            "cpu" -> Quantity(Option(conf.get(ARMADA_DRIVER_REQUEST_CORES).toString))
          )
        )
      )
  }

  private def submitJobs(armadaClient: ArmadaClient, clientArguments: ClientArguments,
    conf: SparkConf): String = {
    val configGenerator =
      new ConfigGenerator("armada-spark-config", conf)
    val driverServiceName = conf.get(DRIVER_SERVICE_NAME_PREFIX) + UUID.randomUUID.toString
    val numExecutors = SchedulerBackendUtils.getInitialTargetExecutorNumber(conf)
    val executorContainers = getExecutorContainers(numExecutors, driverServiceName, conf)

    val gangAnnotations = GangSchedulingAnnotations(None, 1 + numExecutors,
      conf.get(GANG_SCHEDULING_NODE_UNIFORMITY_LABEL))

    val globalLabels = commaSeparatedLabelsToMap(conf.get(ARMADA_SPARK_GLOBAL_LABELS))
    val executorLabels = globalLabels ++ commaSeparatedLabelsToMap(conf.get(ARMADA_SPARK_EXECUTOR_LABELS))
    val executorJobs = for (container <- executorContainers) yield api.submit
      .JobSubmitRequestItem()
      .withPriority(0)
      .withNamespace(conf.get(ARMADA_NAMESPACE))
      .withLabels(executorLabels)
      .withPodSpec(
        PodSpec()
        .withTerminationGracePeriodSeconds(0)
        .withRestartPolicy("Never")
        .withContainers(Seq(container))
        .withVolumes(configGenerator.getVolumes)
        .withNodeSelector(commaSeparatedLabelsToMap(conf.get(ARMADA_CLUSTER_SELECTORS)))
      )
      .withAnnotations(configGenerator.getAnnotations ++ gangAnnotations)

    val podSpec = PodSpec()
      .withTerminationGracePeriodSeconds(0)
      .withRestartPolicy("Never")
      .withContainers(Seq(getDriverContainer(driverServiceName, clientArguments, conf, configGenerator.getVolumeMounts)))
      .withVolumes(configGenerator.getVolumes)
      .withNodeSelector(commaSeparatedLabelsToMap(conf.get(ARMADA_CLUSTER_SELECTORS)))

    val driverLabels = globalLabels ++ commaSeparatedLabelsToMap(conf.get(ARMADA_SPARK_DRIVER_LABELS))
    val driverJob = api.submit
      .JobSubmitRequestItem()
      .withPriority(0)
      .withNamespace(conf.get(ARMADA_NAMESPACE))
      .withLabels(driverLabels)
      .withPodSpec(podSpec)
      .withAnnotations(configGenerator.getAnnotations ++ gangAnnotations)
      .withServices(Seq(
        api.submit.ServiceConfig(
          api.submit.ServiceType.NodePort,
          Seq(DRIVER_PORT),
          driverServiceName)))

    val executorJobsSubmitResponse = armadaClient.submitJobs(conf.get(ARMADA_QUEUE), conf.get(EXECUTOR_JOB_SET_ID), executorJobs)
    for (respItem <- executorJobsSubmitResponse.jobResponseItems) {
      val error = if (respItem.error == "") "None" else respItem.error
      log(s"Executor JobID: ${respItem.jobId}  Error: $error")
    }

    val jobSubmitResponse = armadaClient.submitJobs(conf.get(ARMADA_QUEUE), conf.get(DRIVER_JOB_SET_ID), Seq(driverJob))

    for (respItem <- jobSubmitResponse.jobResponseItems) {
      val error = if (respItem.error == "") "None" else respItem.error
      log(s"Driver JobID: ${respItem.jobId}  Error: $error")
    }
    jobSubmitResponse.jobResponseItems.head.jobId
  }
}
