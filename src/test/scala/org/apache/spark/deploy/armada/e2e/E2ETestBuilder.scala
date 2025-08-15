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

package org.apache.spark.deploy.armada.e2e

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import org.scalatest.Assertions._

/** Fluent API for building E2E tests. Makes it easy for developers to create comprehensive tests
  * for their features.
  *
  * Example usage:
  * {{{
  * E2ETestBuilder("My new feature")
  *   .withSparkConf("spark.my.feature.enabled", "true")
  *   .withExecutors(3)
  *   .assertExecutorCount(3)
  *   .assertDriverHasLabel("feature", "enabled")
  *   .run()
  * }}}
  */
class E2ETestBuilder(testName: String) {
  private var sparkConfs           = Map.empty[String, String]
  private var assertions           = Seq.empty[TestAssertion]
  private var baseQueueName        = "e2e-template"
  private var imageName            = "spark:armada"
  private var masterUrl            = "armada://localhost:30002"
  private var lookoutUrl           = "http://localhost:30000"
  private var scalaVersion         = "2.13"
  private var sparkVersion         = "3.5.5"
  private var failFastOnPodFailure = true

  // Helper to merge labels with existing configuration
  private def addLabels(configKey: String, newLabels: Map[String, String]): E2ETestBuilder = {
    val existing = sparkConfs
      .get(configKey)
      .map { labelStr =>
        labelStr
          .split(",")
          .map { pair =>
            val parts = pair.split("=", 2)
            if (parts.length == 2) parts(0) -> parts(1) else pair -> ""
          }
          .toMap
      }
      .getOrElse(Map.empty)

    val merged   = existing ++ newLabels
    val labelStr = merged.map { case (k, v) => s"$k=$v" }.mkString(",")
    withSparkConf(configKey, labelStr)
  }

  def withSparkConf(key: String, value: String): E2ETestBuilder = {
    sparkConfs += (key -> value)
    this
  }

  def withSparkConf(confs: Map[String, String]): E2ETestBuilder = {
    sparkConfs ++= confs
    this
  }

  def withExecutors(count: Int): E2ETestBuilder = {
    withSparkConf("spark.executor.instances", count.toString)
  }

  /** Enable driver ingress with annotations */
  def withDriverIngress(annotations: Map[String, String] = Map.empty): E2ETestBuilder = {
    withSparkConf("spark.armada.driver.ingress.enabled", "true")
    if (annotations.nonEmpty) {
      withSparkConf(
        "spark.armada.driver.ingress.annotations",
        annotations.map { case (k, v) => s"$k=$v" }.mkString(",")
      )
    }
    this
  }

  /** Use job templates */
  def withJobTemplate(path: String): E2ETestBuilder = {
    withSparkConf("spark.armada.jobTemplate", path)
  }

  def withPodLabels(labels: Map[String, String]): E2ETestBuilder = {
    val labelString = labels.map { case (k, v) => s"$k=$v" }.mkString(",")
    withSparkConf("spark.armada.pod.labels", labelString)
  }

  def withNodeSelectors(selectors: Map[String, String]): E2ETestBuilder = {
    val selectorString = selectors.map { case (k, v) => s"$k=$v" }.mkString(",")
    withSparkConf("spark.armada.scheduling.nodeSelectors", selectorString)
  }

  /** Assert exact executor count */
  def assertExecutorCount(expected: Int): E2ETestBuilder = {
    // Ensure executors have spark-role label for assertions to work
    addLabels("spark.armada.executor.labels", Map("spark-role" -> "executor"))
    assertions :+= new ExecutorCountAssertion(expected)
    this
  }

  def assertDriverExists(): E2ETestBuilder = {
    // Ensure driver has spark-role label for assertions to work
    addLabels("spark.armada.driver.labels", Map("spark-role" -> "driver"))
    assertions :+= new DriverExistsAssertion()
    this
  }

  /** Assert driver has specific label */
  def assertDriverHasLabel(key: String, value: String): E2ETestBuilder = {
    // Ensure driver has spark-role label for assertions to work
    addLabels("spark.armada.driver.labels", Map("spark-role" -> "driver"))
    assertions :+= new DriverLabelAssertion(Map(key -> value))
    this
  }

  /** Assert executors have specific label */
  def assertExecutorsHaveLabel(key: String, value: String): E2ETestBuilder = {
    // Ensure executors have spark-role label for assertions to work
    addLabels("spark.armada.executor.labels", Map("spark-role" -> "executor"))
    assertions :+= new ExecutorLabelAssertion(Map(key -> value))
    this
  }

  /** Assert driver ingress exists with specific annotations (key-value pairs) */
  def assertIngressAnnotations(
      requiredAnnotations: Map[String, String] = Map.empty
  ): E2ETestBuilder = {
    assertions :+= new IngressAssertion(requiredAnnotations)
    this
  }

  /** Assert pods are scheduled with node selectors */
  def assertNodeSelectors(selectors: Map[String, String]): E2ETestBuilder = {
    // We need to create a custom assertion that uses the test-id from context
    assertions :+= new TestAssertion {
      override val name = "Node selector verification"
      override def assert(context: AssertionContext)(implicit
          ec: ExecutionContext
      ): Future[AssertionResult] = {
        // Use test-id from context to find all pods for this test
        val podSelector = s"test-id=${context.testId}"
        new NodeSelectorAssertion(podSelector, selectors).assert(context)
      }
    }
    this
  }

  /** Assert all pods have specific labels */
  def assertPodLabels(labels: Map[String, String]): E2ETestBuilder = {
    labels.foreach { case (key, value) =>
      assertDriverHasLabel(key, value)
      assertExecutorsHaveLabel(key, value)
    }
    this
  }

  /** Assert driver pod has all specified labels */
  def assertDriverHasLabels(labels: Map[String, String]): E2ETestBuilder = {
    labels.foreach { case (key, value) =>
      assertDriverHasLabel(key, value)
    }
    this
  }

  /** Assert executor pods have all specified labels */
  def assertExecutorsHaveLabels(labels: Map[String, String]): E2ETestBuilder = {
    labels.foreach { case (key, value) =>
      assertExecutorsHaveLabel(key, value)
    }
    this
  }

  /** Assert driver has specific annotation */
  def assertDriverHasAnnotation(key: String, value: String): E2ETestBuilder = {
    // Ensure driver has spark-role label for assertions to work
    addLabels("spark.armada.driver.labels", Map("spark-role" -> "driver"))
    assertions :+= new DriverAnnotationAssertion(Map(key -> value))
    this
  }

  /** Assert executors have specific annotation */
  def assertExecutorsHaveAnnotation(key: String, value: String): E2ETestBuilder = {
    // Ensure executors have spark-role label for assertions to work
    addLabels("spark.armada.executor.labels", Map("spark-role" -> "executor"))
    assertions :+= new ExecutorAnnotationAssertion(Map(key -> value))
    this
  }

  /** Assert driver pod has all specified annotations */
  def assertDriverHasAnnotations(annotations: Map[String, String]): E2ETestBuilder = {
    annotations.foreach { case (key, value) =>
      assertDriverHasAnnotation(key, value)
    }
    this
  }

  /** Assert executor pods have all specified annotations */
  def assertExecutorsHaveAnnotations(annotations: Map[String, String]): E2ETestBuilder = {
    annotations.foreach { case (key, value) =>
      assertExecutorsHaveAnnotation(key, value)
    }
    this
  }

  def build(): TestConfig = {
    TestConfig(
      baseQueueName = baseQueueName,
      imageName = imageName,
      masterUrl = masterUrl,
      lookoutUrl = lookoutUrl,
      scalaVersion = scalaVersion,
      sparkVersion = sparkVersion,
      sparkConfs = sparkConfs,
      assertions = assertions,
      failFastOnPodFailure = failFastOnPodFailure
    )
  }

  /** Configure base settings from existing config */
  def withBaseConfig(config: TestConfig): E2ETestBuilder = {
    this.baseQueueName = config.baseQueueName
    this.imageName = config.imageName
    this.masterUrl = config.masterUrl
    this.lookoutUrl = config.lookoutUrl
    this.scalaVersion = config.scalaVersion
    this.sparkVersion = config.sparkVersion
    this.sparkConfs ++= config.sparkConfs
    this.failFastOnPodFailure = config.failFastOnPodFailure
    this
  }

  def run()(implicit orchestrator: TestOrchestrator): TestResult = {
    import scala.concurrent.Await
    val future = orchestrator.runTest(testName, build())
    val result = Await.result(future, 5.minutes)

    assert(result.status == JobSetStatus.Success, s"Job failed with status: ${result.status}")

    result.assertionResults.foreach { case (name, assertionResult) =>
      assertionResult match {
        case AssertionResult.Success => // Success, no need to log
        case AssertionResult.Failure(msg, _) =>
          fail(s"Assertion '$name' failed: $msg")
      }
    }

    result
  }
}

/** Companion object with factory methods */
object E2ETestBuilder {
  def apply(testName: String): E2ETestBuilder = new E2ETestBuilder(testName)
}
