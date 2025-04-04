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
import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkApplication
import org.apache.spark.deploy.armada.Config.{ARMADA_HEALTH_CHECK_TIMEOUT, ARMADA_LOOKOUTURL}

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
    val jobId = submitDriverJob(armadaClient, clientArguments, sparkConf)
    log(s"Got job ID: $jobId")

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

  private def submitDriverJob(armadaClient: ArmadaClient, clientArguments: ClientArguments,
    conf: SparkConf): String = {
    val source = EnvVarSource().withFieldRef(ObjectFieldSelector()
      .withApiVersion("v1").withFieldPath("status.podIP"))
    val envVars = Seq(
      new EnvVar().withName("SPARK_DRIVER_BIND_ADDRESS").withValueFrom(source),
      new EnvVar().withName(ConfigGenerator.ENV_SPARK_CONF_DIR).withValue(ConfigGenerator.REMOTE_CONF_DIR_NAME),
      new EnvVar().withName("EXTERNAL_CLUSTER_SUPPORT_ENABLED").withValue("true")
    )

    val configGenerator =
      new ConfigGenerator("armada-spark-config", conf)
    val primaryResource = clientArguments.mainAppResource match {
      case JavaMainAppResource(Some(resource)) => Seq(resource)
      case PythonMainAppResource(resource) => Seq(resource)
      case RMainAppResource(resource) => Seq(resource)
      case _ => Seq()
    }

    val driverContainer = Container()
      .withName("spark-driver")
      .withImagePullPolicy("IfNotPresent")
      .withImage(conf.get("spark.kubernetes.container.image"))
      .withEnv(envVars)
      .withCommand(Seq("/opt/entrypoint.sh"))
      .withVolumeMounts(configGenerator.getVolumeMounts)
      .withArgs(
        Seq(
          "driver",
          "--verbose",
          "--class",
          clientArguments.mainClass,
          "--master",
          "local://armada://armada-server.armada.svc.cluster.local:50051",
          "--conf",
          s"spark.kubernetes.container.image=${conf.get("spark.kubernetes.container.image")}",
          "--conf",
          "spark.driver.port=7078",
          "--conf",
          "spark.driver.host=$(SPARK_DRIVER_BIND_ADDRESS)"

        ) ++ primaryResource ++ clientArguments.driverArgs
      )
      .withResources( // FIXME: What are reasonable requests/limits for spark drivers?
        ResourceRequirements(
          limits = Map(
            "memory" -> Quantity(Option("450Mi")),
            "ephemeral-storage" -> Quantity(Option("512Mi")),
            "cpu" -> Quantity(Option("200m"))
          ),
          requests = Map(
            "memory" -> Quantity(Option("450Mi")),
            "ephemeral-storage" -> Quantity(Option("512Mi")),
            "cpu" -> Quantity(Option("200m"))
          )
        )
      )

    val podSpec = PodSpec()
      .withTerminationGracePeriodSeconds(0)
      .withRestartPolicy("Never")
      .withContainers(Seq(driverContainer))
      .withVolumes(configGenerator.getVolumes)

    val driverJob = api.submit
      .JobSubmitRequestItem()
      .withPriority(0)
      .withNamespace("default")
      .withPodSpec(podSpec)
      .withAnnotations(configGenerator.getAnnotations)

    // FIXME: Plumb config for queue, job-set-id
    val jobSubmitResponse = armadaClient.submitJobs("test", "driver", Seq(driverJob))

    for (respItem <- jobSubmitResponse.jobResponseItems) {
      val error = if (respItem.error == "") "None" else respItem.error
      log(s"JobID: ${respItem.jobId}  Error: $error")
    }
    jobSubmitResponse.jobResponseItems.head.jobId
  }
}
