package org.apache.spark.deploy.armada.e2e

import org.scalatest.funsuite.AnyFunSuite
import scala.sys.process._

class DiagnosticTest extends AnyFunSuite {

  test("Environment Diagnostics") {
    println("\n[DIAG] ========== ENVIRONMENT DIAGNOSTICS ==========")

    // Check armadactl availability
    println("[DIAG] Checking armadactl...")
    try {
      val armadactlPath = sys.props.get("armadactl.path").getOrElse("armadactl")
      val versionResult = s"$armadactlPath version".!!
      println(s"[DIAG] armadactl found at: $armadactlPath")
      println(s"[DIAG] armadactl version: $versionResult")
    } catch {
      case e: Exception =>
        println(s"[DIAG] ERROR: armadactl not found or not executable: ${e.getMessage}")
    }

    // Check Armada connectivity
    println("\n[DIAG] Checking Armada connectivity...")
    try {
      val armadactlPath = sys.props.get("armadactl.path").getOrElse("armadactl")
      val queuesResult  = s"$armadactlPath get queues --armadaUrl localhost:30002".!!
      println(s"[DIAG] Successfully connected to Armada")
      println(s"[DIAG] Existing queues:\n$queuesResult")
    } catch {
      case e: Exception =>
        println(s"[DIAG] ERROR: Cannot connect to Armada: ${e.getMessage}")
    }

    // Test queue creation
    println("\n[DIAG] Testing queue creation...")
    val testQueueName = s"diag-test-${System.currentTimeMillis()}"
    try {
      val armadactlPath = sys.props.get("armadactl.path").getOrElse("armadactl")
      val createResult =
        s"$armadactlPath create queue $testQueueName --armadaUrl localhost:30002".!!
      println(s"[DIAG] Successfully created test queue: $testQueueName")

      // Try to get the queue
      Thread.sleep(2000) // Wait for eventual consistency
      val getResult = s"$armadactlPath get queue $testQueueName --armadaUrl localhost:30002".!!
      println(s"[DIAG] Successfully retrieved test queue")

      // Clean up
      try {
        s"$armadactlPath delete queue $testQueueName --armadaUrl localhost:30002".!!
        println(s"[DIAG] Successfully deleted test queue")
      } catch {
        case _: Exception => // Ignore cleanup errors
      }
    } catch {
      case e: Exception =>
        println(s"[DIAG] ERROR: Cannot create queue: ${e.getMessage}")
        e.printStackTrace()
    }

    println("[DIAG] ========== END DIAGNOSTICS ==========\n")
  }
}
