package org.apache.spark.deploy.armada.submit

import org.apache.spark.SparkConf
import org.apache.spark.deploy.armada.Config
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks

import java.io.File
import scala.collection.mutable.ListBuffer
import scala.sys.process._

private class ArmadaUtilsSuite
    extends AnyFunSuite
    with TableDrivenPropertyChecks
    with Matchers
    with BeforeAndAfterEach {
  var tempDir: File        = _
  var sparkConf: SparkConf = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    tempDir = java.nio.file.Files.createTempDirectory("armada-auth-test").toFile
    tempDir.deleteOnExit()
    sparkConf = new SparkConf(false)
  }

  override def afterEach(): Unit = {
    if (tempDir != null && tempDir.exists()) {
      tempDir.listFiles().foreach(_.delete())
      tempDir.delete()
    }
    super.afterEach()
  }
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

  test("getAuthToken returns None when config is None") {
    val token = ArmadaUtils.getAuthToken(None)
    assert(token === None)
  }

  test("getAuthToken returns None when script path config is not set") {
    val token = ArmadaUtils.getAuthToken(Some(sparkConf))
    assert(token === None)
  }

  test("getAuthToken throws RuntimeException when script file does not exist") {
    val nonExistentPath = new File(tempDir, "non-existent-script.sh").getAbsolutePath
    sparkConf.set(Config.ARMADA_AUTH_SCRIPT_PATH.key, nonExistentPath)

    val exception = intercept[RuntimeException] {
      ArmadaUtils.getAuthToken(Some(sparkConf))
    }
    assert(exception.getMessage.contains("Authentication script does not exist"))
    assert(exception.getMessage.contains(nonExistentPath))
  }

  test("getAuthToken throws RuntimeException when script exists but is not executable") {
    val scriptFile = new File(tempDir, "non-executable-script.sh")
    java.nio.file.Files.write(
      scriptFile.toPath,
      "#!/bin/sh\necho 'token'".getBytes
    )
    // no executable permissions set
    sparkConf.set(Config.ARMADA_AUTH_SCRIPT_PATH.key, scriptFile.getAbsolutePath)

    val exception = intercept[RuntimeException] {
      ArmadaUtils.getAuthToken(Some(sparkConf))
    }
    assert(exception.getMessage.contains("Authentication script is not executable"))
    assert(exception.getMessage.contains(scriptFile.getAbsolutePath))
  }

  test("getAuthToken returns token when script executes successfully") {
    val scriptFile = new File(tempDir, "auth-script.sh")
    java.nio.file.Files.write(
      scriptFile.toPath,
      "#!/bin/sh\necho 'test-token-123'".getBytes
    )
    scriptFile.setExecutable(true)
    sparkConf.set(Config.ARMADA_AUTH_SCRIPT_PATH.key, scriptFile.getAbsolutePath)

    val token = ArmadaUtils.getAuthToken(Some(sparkConf))
    assert(token === Some("test-token-123"))
  }

  test("getAuthToken returns token with whitespace trimmed") {
    val scriptFile = new File(tempDir, "auth-script.sh")
    java.nio.file.Files.write(
      scriptFile.toPath,
      "#!/bin/sh\necho '  token-with-spaces  '".getBytes
    )
    scriptFile.setExecutable(true)
    sparkConf.set(Config.ARMADA_AUTH_SCRIPT_PATH.key, scriptFile.getAbsolutePath)

    val token = ArmadaUtils.getAuthToken(Some(sparkConf))
    assert(token === Some("token-with-spaces"))
  }

  test("getAuthToken throws RuntimeException when script returns non-zero exit code") {
    val scriptFile = new File(tempDir, "failing-script.sh")
    java.nio.file.Files.write(
      scriptFile.toPath,
      "#!/bin/sh\necho 'error' >&2\nexit 1".getBytes
    )
    scriptFile.setExecutable(true)
    sparkConf.set(Config.ARMADA_AUTH_SCRIPT_PATH.key, scriptFile.getAbsolutePath)

    val exception = intercept[RuntimeException] {
      ArmadaUtils.getAuthToken(Some(sparkConf))
    }
    assert(exception.getMessage.contains("Authentication script returned non-zero exit code: 1"))
  }

  test("getAuthToken throws RuntimeException when script returns empty output") {
    val scriptFile = new File(tempDir, "empty-output-script.sh")
    java.nio.file.Files.write(
      scriptFile.toPath,
      "#!/bin/sh\necho ''".getBytes
    )
    scriptFile.setExecutable(true)
    sparkConf.set(Config.ARMADA_AUTH_SCRIPT_PATH.key, scriptFile.getAbsolutePath)

    val exception = intercept[RuntimeException] {
      ArmadaUtils.getAuthToken(Some(sparkConf))
    }
    assert(exception.getMessage.contains("Authentication script returned empty output"))
  }

  test("getAuthToken throws RuntimeException when script returns only whitespace") {
    val scriptFile = new File(tempDir, "whitespace-script.sh")
    java.nio.file.Files.write(
      scriptFile.toPath,
      "#!/bin/sh\necho '   \n\t  '".getBytes
    )
    scriptFile.setExecutable(true)
    sparkConf.set(Config.ARMADA_AUTH_SCRIPT_PATH.key, scriptFile.getAbsolutePath)

    val exception = intercept[RuntimeException] {
      ArmadaUtils.getAuthToken(Some(sparkConf))
    }
    assert(exception.getMessage.contains("Authentication script returned empty output"))
  }

  test("getAuthToken handles multi-line output correctly") {
    val scriptFile = new File(tempDir, "multiline-script.sh")
    java.nio.file.Files.write(
      scriptFile.toPath,
      "#!/bin/sh\necho 'line1'\necho 'line2'".getBytes
    )
    scriptFile.setExecutable(true)
    sparkConf.set(Config.ARMADA_AUTH_SCRIPT_PATH.key, scriptFile.getAbsolutePath)

    val token = ArmadaUtils.getAuthToken(Some(sparkConf))
    // Should capture all output, newlines included
    assert(token.isDefined)
    assert(token.get.contains("line1"))
    assert(token.get.contains("line2"))
  }

  test("getAuthToken throws RuntimeException when script contains invalid command") {
    val scriptFile = new File(tempDir, "invalid-script.sh")
    // Create a script with a command that doesn't exist (returns exit code 127)
    java.nio.file.Files.write(
      scriptFile.toPath,
      "#!/bin/sh\ninvalid-command-that-does-not-exist-xyz123".getBytes
    )
    scriptFile.setExecutable(true)
    sparkConf.set(Config.ARMADA_AUTH_SCRIPT_PATH.key, scriptFile.getAbsolutePath)

    val exception = intercept[RuntimeException] {
      ArmadaUtils.getAuthToken(Some(sparkConf))
    }
    assert(exception.getMessage.contains("Authentication script returned non-zero exit code: 127"))
  }

  test("executeScript returns output when script executes successfully") {
    val scriptFile = new File(tempDir, "test-script.sh")
    java.nio.file.Files.write(
      scriptFile.toPath,
      "#!/bin/sh\necho 'test-output-123'".getBytes
    )
    scriptFile.setExecutable(true)

    val output = ArmadaUtils.executeScript(scriptFile)
    assert(output === Some("test-output-123"))
  }

  test("executeScript returns output with whitespace trimmed") {
    val scriptFile = new File(tempDir, "whitespace-script.sh")
    java.nio.file.Files.write(
      scriptFile.toPath,
      "#!/bin/sh\necho '  output-with-spaces  '".getBytes
    )
    scriptFile.setExecutable(true)

    val output = ArmadaUtils.executeScript(scriptFile)
    assert(output === Some("output-with-spaces"))
  }

  test("executeScript handles multi-line output correctly") {
    val scriptFile = new File(tempDir, "multiline-script.sh")
    java.nio.file.Files.write(
      scriptFile.toPath,
      "#!/bin/sh\necho 'line1'\necho 'line2'".getBytes
    )
    scriptFile.setExecutable(true)

    val output = ArmadaUtils.executeScript(scriptFile)
    assert(output.isDefined)
    assert(output.get.contains("line1"))
    assert(output.get.contains("line2"))
  }

  test("executeScript throws RuntimeException when script returns non-zero exit code") {
    val scriptFile = new File(tempDir, "failing-script.sh")
    java.nio.file.Files.write(
      scriptFile.toPath,
      "#!/bin/sh\necho 'error' >&2\nexit 1".getBytes
    )
    scriptFile.setExecutable(true)

    val exception = intercept[RuntimeException] {
      ArmadaUtils.executeScript(scriptFile)
    }
    assert(exception.getMessage.contains("Script returned non-zero exit code: 1"))
    assert(exception.getMessage.contains(scriptFile.getAbsolutePath))
    assert(exception.getMessage.contains("stderr: error"))
  }

  test("executeScript throws RuntimeException when script returns empty output") {
    val scriptFile = new File(tempDir, "empty-output-script.sh")
    java.nio.file.Files.write(
      scriptFile.toPath,
      "#!/bin/sh\necho ''".getBytes
    )
    scriptFile.setExecutable(true)

    val exception = intercept[RuntimeException] {
      ArmadaUtils.executeScript(scriptFile)
    }
    assert(exception.getMessage.contains("Script returned empty output"))
    assert(exception.getMessage.contains(scriptFile.getAbsolutePath))
  }

  test("executeScript throws RuntimeException when script returns only whitespace") {
    val scriptFile = new File(tempDir, "whitespace-only-script.sh")
    java.nio.file.Files.write(
      scriptFile.toPath,
      "#!/bin/sh\necho '   \n\t  '".getBytes
    )
    scriptFile.setExecutable(true)

    val exception = intercept[RuntimeException] {
      ArmadaUtils.executeScript(scriptFile)
    }
    assert(exception.getMessage.contains("Script returned empty output"))
  }

  test("executeScript uses custom scriptContext in error messages") {
    val scriptFile = new File(tempDir, "failing-script.sh")
    java.nio.file.Files.write(
      scriptFile.toPath,
      "#!/bin/sh\nexit 1".getBytes
    )
    scriptFile.setExecutable(true)

    val exception = intercept[RuntimeException] {
      ArmadaUtils.executeScript(scriptFile, "Custom validation script")
    }
    assert(exception.getMessage.contains("Custom validation script returned non-zero exit code: 1"))
  }

  test("executeScript uses default scriptContext when not provided") {
    val scriptFile = new File(tempDir, "failing-script.sh")
    java.nio.file.Files.write(
      scriptFile.toPath,
      "#!/bin/sh\nexit 1".getBytes
    )
    scriptFile.setExecutable(true)

    val exception = intercept[RuntimeException] {
      ArmadaUtils.executeScript(scriptFile)
    }
    assert(exception.getMessage.contains("Script returned non-zero exit code: 1"))
  }

  test("executeScript includes stderr in error message when available") {
    val scriptFile = new File(tempDir, "stderr-script.sh")
    java.nio.file.Files.write(
      scriptFile.toPath,
      "#!/bin/sh\necho 'error message' >&2\nexit 2".getBytes
    )
    scriptFile.setExecutable(true)

    val exception = intercept[RuntimeException] {
      ArmadaUtils.executeScript(scriptFile)
    }
    assert(exception.getMessage.contains("stderr: error message"))
  }

  test("executeScript does not include stderr in error message when empty") {
    val scriptFile = new File(tempDir, "no-stderr-script.sh")
    java.nio.file.Files.write(
      scriptFile.toPath,
      "#!/bin/sh\nexit 3".getBytes
    )
    scriptFile.setExecutable(true)

    val exception = intercept[RuntimeException] {
      ArmadaUtils.executeScript(scriptFile)
    }
    assert(exception.getMessage.contains("Script returned non-zero exit code: 3"))
    assert(!exception.getMessage.contains("stderr:"))
  }
}
