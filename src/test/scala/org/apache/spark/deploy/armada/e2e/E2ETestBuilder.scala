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

  /** Add multiple Spark configurations (alias for consistency) */
  def withSparkConfs(confs: Map[String, String]): E2ETestBuilder = {
    withSparkConf(confs)
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
    withSparkConf("spark.armada.node.selectors", selectorString)
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

  /** Assert driver ingress exists, optionally with specific annotations */
  def assertIngress(requiredAnnotations: Set[String] = Set.empty): E2ETestBuilder = {
    // Always check for basic Armada annotations plus any additional ones specified
    val baseAnnotations = Set("armada_jobset_id", "armada_owner")
    assertions :+= new IngressAssertion(baseAnnotations ++ requiredAnnotations)
    this
  }

  /** Assert driver ingress exists (convenience overload with no annotations) */
  def assertIngress(): E2ETestBuilder = {
    assertIngress(Set.empty)
  }

  /** Assert pods are scheduled with node selectors */
  def assertNodeSelectors(selectors: Map[String, String]): E2ETestBuilder = {
    assertions :+= new NodeSelectorAssertion(
      podSelector = "spark-role=driver,spark-role=executor",
      expectedNodeSelectors = selectors
    )
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

  def withAssertion(assertion: TestAssertion): E2ETestBuilder = {
    assertions :+= assertion
    this
  }

  /** Add custom assertion with inline definition */
  def assertThat(assertionName: String)(check: AssertionContext => Boolean): E2ETestBuilder = {
    assertions :+= new TestAssertion {
      override val name: String = assertionName
      override def assert(context: AssertionContext)(implicit
          ec: ExecutionContext
      ): Future[AssertionResult] = {
        Future {
          if (check(context)) AssertionResult.Success
          else AssertionResult.Failure(s"Assertion '$assertionName' failed")
        }
      }
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
