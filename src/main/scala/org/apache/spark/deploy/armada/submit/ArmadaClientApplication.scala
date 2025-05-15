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
  SPARK_DRIVER_SERVICE_NAME_PREFIX,
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

import scala.util.Random

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
    validateConfig(sparkConf)

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
    val (driverJobId, executorJobIds) = submitArmadaJob(armadaClient, clientArguments, sparkConf)

    val lookoutBaseURL = sparkConf.get(ARMADA_LOOKOUTURL)
    val lookoutURL =
      s"$lookoutBaseURL/?page=0&sort[id]=jobId&sort[desc]=true&" +
        s"ps=50&sb=$driverJobId&active=false&refresh=true"
    log(s"Lookout URL for the driver job is $lookoutURL")

    ()
  }

  private def validateConfig(conf: SparkConf): Unit = {
    val nodeSelectors       = commaSeparatedLabelsToMap(conf.get(ARMADA_JOB_NODE_SELECTORS))
    val gangUniformityLabel = conf.get(ARMADA_JOB_GANG_SCHEDULING_NODE_UNIFORMITY)

    if (nodeSelectors.isEmpty && gangUniformityLabel.isEmpty) {
      throw new IllegalArgumentException(
        "Either node selectors or gang scheduling node uniformity label must be set."
      )
    }
  }

  private def submitArmadaJob(
      armadaClient: ArmadaClient,
      clientArguments: ClientArguments,
      conf: SparkConf
  ): (String, Seq[String]) = {
    val queue               = conf.get(ARMADA_JOB_QUEUE)
    val jobSetId            = conf.get(ARMADA_JOB_SET_ID)
    val (driver, executors) = newSparkJobSubmitRequestItems(clientArguments, conf)

    val driverResponse = armadaClient.submitJobs(queue, jobSetId, Seq(driver))
    val driverJobId    = driverResponse.jobResponseItems.head.jobId
    log(
      s"Submitted driver job with ID: $driverJobId, Error: ${driverResponse.jobResponseItems.head.error}"
    )

    val executorsResponse = armadaClient.submitJobs(queue, jobSetId, executors)
    val executorJobIds = executorsResponse.jobResponseItems.map(item => {
      log(
        s"Submitted executor job with ID: ${item.jobId}, Error: ${item.error}"
      )
      item.jobId
    })

    (driverJobId, executorJobIds)
  }

  private def newSparkJobSubmitRequestItems(
      clientArguments: ClientArguments,
      conf: SparkConf
  ): (api.submit.JobSubmitRequestItem, Seq[api.submit.JobSubmitRequestItem]) = {

    val namespace = conf.get(ARMADA_SPARK_JOB_NAMESPACE)
    val priority  = conf.get(ARMADA_SPARK_JOB_PRIORITY)

    val primaryResource = clientArguments.mainAppResource match {
      case JavaMainAppResource(Some(resource)) => Seq(resource)
      case PythonMainAppResource(resource)     => Seq(resource)
      case RMainAppResource(resource)          => Seq(resource)
      case _                                   => Seq()
    }

    val master = conf.get("spark.master")
    val internalMaster =
      if (conf.get(ARMADA_SERVER_INTERNAL_URL).nonEmpty)
        s"local://armada://${conf.get(ARMADA_SERVER_INTERNAL_URL)}"
      else {
        master
      }

    val image         = conf.get("spark.kubernetes.container.image")
    val executorCount = SchedulerBackendUtils.getInitialTargetExecutorNumber(conf)

    val configGenerator = new ConfigGenerator("armada-spark-config", conf)
    val gangAnnotations = GangSchedulingAnnotations(
      None,
      1 + executorCount,
      conf.get(ARMADA_JOB_GANG_SCHEDULING_NODE_UNIFORMITY)
    )
    val annotations  = configGenerator.getAnnotations ++ gangAnnotations
    val globalLabels = commaSeparatedLabelsToMap(conf.get(ARMADA_SPARK_POD_LABELS))
    val driverLabels =
      globalLabels ++ commaSeparatedLabelsToMap(conf.get(ARMADA_SPARK_DRIVER_LABELS))
    val nodeSelectors = commaSeparatedLabelsToMap(conf.get(ARMADA_JOB_NODE_SELECTORS))

    val driverServiceName = conf.get(SPARK_DRIVER_SERVICE_NAME_PREFIX) + Utils.randAlphanum()
    val driverPort        = 7078

    val confSeq = conf.getAll.flatMap { case (k, v) =>
      Seq("--conf", s"$k=$v")
    }
    val driver = newSparkDriverJobSubmitRequestItem(
      internalMaster,
      namespace,
      priority,
      annotations,
      driverLabels,
      image,
      driverPort,
      driverServiceName,
      clientArguments.mainClass,
      configGenerator.getVolumes,
      configGenerator.getVolumeMounts,
      nodeSelectors,
      confSeq ++ primaryResource ++ clientArguments.driverArgs
    )

    val executorLabels =
      globalLabels ++ commaSeparatedLabelsToMap(conf.get(ARMADA_SPARK_EXECUTOR_LABELS))
    val nodeUniformityLabel = conf.get(ARMADA_JOB_GANG_SCHEDULING_NODE_UNIFORMITY)
    val executors = (0 until executorCount).map { index =>
      newExecutorJobSubmitItem(
        index,
        priority,
        namespace,
        annotations,
        executorLabels,
        image,
        javaOptEnvVars(conf),
        nodeUniformityLabel,
        driverServiceName,
        driverPort,
        configGenerator.getVolumes,
        nodeSelectors
      )
    }

    (driver, executors)
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
      serviceName: String,
      mainClass: String,
      volumes: Seq[Volume],
      volumeMounts: Seq[VolumeMount],
      nodeSelectors: Map[String, String],
      additionalDriverArgs: Seq[String]
  ): api.submit.JobSubmitRequestItem = {
    val container = newSparkDriverContainer(
      master,
      driverImage,
      driverPort,
      mainClass,
      serviceName,
      volumeMounts,
      additionalDriverArgs
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
            Seq(driverPort),
            serviceName
          )
        )
      )
  }

  private def newSparkDriverContainer(
      master: String,
      image: String,
      port: Int,
      mainClass: String,
      serviceName: String,
      volumeMounts: Seq[VolumeMount],
      additionalDriverArgs: Seq[String]
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
        .withValue(ConfigGenerator.REMOTE_CONF_DIR_NAME),
      EnvVar()
        .withName("EXTERNAL_CLUSTER_SUPPORT_ENABLED")
        .withValue("true"),
      EnvVar()
        .withName("ARMADA_SPARK_DRIVER_SERVICE_NAME")
        .withValue(serviceName)
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
          requests = defaultDriverResources,
          limits = defaultDriverResources
        )
      )
  }

  private val defaultDriverResources = Map(
    "memory" -> Quantity(Option("512Mi")),
    "cpu"    -> Quantity(Option("250m"))
  )

  private def newExecutorJobSubmitItem(
      index: Int,
      priority: Double,
      namespace: String,
      annotations: Map[String, String],
      labels: Map[String, String],
      image: String,
      javaOptEnvVars: Seq[EnvVar],
      nodeUniformityLabel: String,
      driverHostname: String,
      driverPort: Int,
      volumes: Seq[Volume],
      nodeSelectors: Map[String, String]
  ): api.submit.JobSubmitRequestItem = {
    val initContainer = newExecutorInitContainer(driverHostname, driverPort)
    val container = newExecutorContainer(
      index,
      image,
      driverHostname,
      driverPort,
      nodeUniformityLabel,
      javaOptEnvVars
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

  private def newExecutorInitContainer(driverHost: String, driverPort: Int) = {
    Container()
      .withName("init")
      .withImagePullPolicy("IfNotPresent")
      .withImage("busybox")
      .withEnv(
        Seq(
          EnvVar().withName("SPARK_DRIVER_HOST").withValue(driverHost),
          EnvVar().withName("SPARK_DRIVER_PORT").withValue(driverPort.toString)
        )
      )
      .withCommand(Seq("sh", "-c"))
      .withArgs(
        Seq(
          "until nc -z $SPARK_DRIVER_HOST $SPARK_DRIVER_PORT; do " +
            "echo \"waiting for driver...\"; sleep 1; done"
        )
      )
  }

  private def newExecutorContainer(
      index: Int,
      image: String,
      driverHostname: String,
      driverPort: Int,
      nodeUniformityLabel: String,
      javaOptEnvVars: Seq[EnvVar]
  ): Container = {
    val driverURL = s"spark://CoarseGrainedScheduler@$driverHostname:$driverPort"
    val source = EnvVarSource().withFieldRef(
      ObjectFieldSelector()
        .withApiVersion("v1")
        .withFieldPath("status.podIP")
    )
    val envVars = Seq(
      EnvVar().withName("SPARK_EXECUTOR_ID").withValue(index.toString),
      EnvVar().withName("SPARK_RESOURCE_PROFILE_ID").withValue("0"),
      EnvVar().withName("SPARK_EXECUTOR_POD_NAME").withValue("test-pod-name"),
      EnvVar().withName("SPARK_APPLICATION_ID").withValue("test_spark_app_id"),
      EnvVar().withName("SPARK_EXECUTOR_CORES").withValue("1"),
      EnvVar().withName("SPARK_EXECUTOR_MEMORY").withValue("512m"),
      EnvVar().withName("SPARK_DRIVER_URL").withValue(driverURL),
      EnvVar().withName("SPARK_EXECUTOR_POD_IP").withValueFrom(source),
      EnvVar().withName("ARMADA_SPARK_GANG_NODE_UNIFORMITY_LABEL").withValue(nodeUniformityLabel)
    )
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
          requests = defaultExecutorResources,
          limits = defaultExecutorResources
        )
      )
  }

  private val defaultExecutorResources = Map(
    "memory" -> Quantity(Option("512Mi")),
    "cpu"    -> Quantity(Option("250m"))
  )
}

object Utils {
  // define the allowed characters: a–z and 0–9
  private val suffixChars: IndexedSeq[Char] =
    ('a' to 'z') ++ ('0' to '9')

  /** Generates a random lowercase alphanumeric string of the specified length.
    *
    * This function picks characters uniformly at random from the set [a–z0–9] and concatenates them
    * into a single string.
    *
    * @param length
    *   the length of the resulting string (default is 5)
    * @return
    *   a randomly generated string containing only lowercase letters and digits, e.g. "a3k9q"
    */
  def randAlphanum(length: Int = 5): String =
    (1 to length)
      .map(_ => suffixChars(Random.nextInt(suffixChars.length)))
      .mkString
}
