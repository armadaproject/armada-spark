package org.apache.spark.deploy.armada.submit

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks

import scala.collection.mutable.ListBuffer
import scala.sys.process._

private class ArmadaUtilsSuite extends AnyFunSuite with TableDrivenPropertyChecks with Matchers {
  test("buildServiceNameFromJobId should create a valid service name") {
    val input    = "01jwefnvywg1sa05m593jztcce"
    val expected = "armada-01jwefnvywg1sa05m593jztcce-0-service-0"
    val actual   = ArmadaUtils.buildServiceNameFromJobId(input)
    assert(actual == expected)
  }

  test("Confirm initContainer sh command succeeds with server") {
    // start server
    val serverPort    = "54525"
    val serverCommand = Seq("nc", "-l", "-p", serverPort)
    val server        = Process.apply(serverCommand).run

    // start client
    val containerCommand = Seq("sh", "-c", ArmadaUtils.initContainerCommand)
    try {
      val client = Process
        .apply(
          containerCommand,
          None,
          ("SPARK_EXECUTOR_CONNECTION_TIMEOUT", "5"),
          ("SPARK_DRIVER_HOST", "localhost"),
          ("SPARK_DRIVER_PORT", serverPort)
        )
        .run
      assert(client.exitValue == 0)
    } finally {
      server.destroy()
    }
  }

  test("Confirm initContainer sh command fails with no server") {
    val serverPort       = "54526"
    val timeout          = "5"
    val containerCommand = Seq("sh", "-c", ArmadaUtils.initContainerCommand)
    val client = Process.apply(
      containerCommand,
      None,
      ("SPARK_EXECUTOR_CONNECTION_TIMEOUT", timeout),
      ("SPARK_DRIVER_HOST", "localhost"),
      ("SPARK_DRIVER_PORT", serverPort)
    )
    val stringBuffer = ListBuffer.empty[String]
    assertThrows[RuntimeException](client.lineStream.foreach(stringBuffer += _))
    val finalList = stringBuffer.toList
    assert(finalList.contains(s"Timeout waiting for driver after ${timeout}s"))
  }

}
