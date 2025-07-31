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

import java.util.concurrent.TimeoutException
import scala.concurrent.{Future, ExecutionContext, blocking}
import scala.concurrent.duration._
import scala.sys.process._
import scala.util.{Failure, Success, Try}

case class ProcessResult(
    exitCode: Int,
    stdout: String,
    stderr: String,
    timedOut: Boolean = false
)

object ProcessExecutor {
  def execute(command: Seq[String], timeout: Duration): ProcessResult = {
    val stdout = new StringBuilder
    val stderr = new StringBuilder

    val processLogger = ProcessLogger(
      line => stdout.append(line).append("\n"),
      line => stderr.append(line).append("\n")
    )

    @volatile var timedOut = false
    val process            = Process(command).run(processLogger)

    Try {
      import ExecutionContext.Implicits.global
      val exitCodeFuture = Future(blocking(process.exitValue()))

      Try(concurrent.Await.result(exitCodeFuture, timeout)) match {
        case Success(exitCode) =>
          ProcessResult(exitCode, stdout.toString.trim, stderr.toString.trim)
        case Failure(_: TimeoutException) =>
          timedOut = true
          process.destroy()
          process.exitValue() // Wait for termination
          ProcessResult(-1, stdout.toString.trim, stderr.toString.trim, timedOut = true)
        case Failure(ex) =>
          throw ex
      }
    } match {
      case Success(result) if result.exitCode == 0 => result
      case Success(result) if result.timedOut =>
        throw new TimeoutException(s"Process timed out after $timeout: ${command.mkString(" ")}")
      case Success(result) =>
        println(s"[PROCESS] Command failed: ${command.take(3).mkString(" ")}...")
        println(s"[PROCESS] Exit code: ${result.exitCode}")
        if (result.stdout.nonEmpty) {
          println(s"[PROCESS] Stdout: ${result.stdout.take(1000)}")
        }
        if (result.stderr.nonEmpty) {
          println(s"[PROCESS] Stderr: ${result.stderr.take(1000)}")
        }
        throw new ProcessExecutionException(
          s"Process failed with exit code ${result.exitCode}",
          result
        )
      case Failure(ex) =>
        Try(process.destroy())
        throw ex
    }
  }

  def executeAsync(command: Seq[String]): ProcessHandle = {
    val stdout = new StringBuilder
    val stderr = new StringBuilder

    val processLogger = ProcessLogger(
      line => stdout.append(line).append("\n"),
      line => stderr.append(line).append("\n")
    )

    val process = Process(command).run(processLogger)
    ProcessHandle(process, stdout, stderr)
  }
}

case class ProcessHandle(
    process: scala.sys.process.Process,
    stdout: StringBuilder,
    stderr: StringBuilder
) {
  def waitFor(timeout: Duration)(implicit ec: ExecutionContext): ProcessResult = {
    val exitCodeFuture = Future(blocking(process.exitValue()))

    Try(concurrent.Await.result(exitCodeFuture, timeout)) match {
      case Success(exitCode) =>
        ProcessResult(
          exitCode,
          stdout.toString.trim,
          stderr.toString.trim
        )
      case Failure(_: TimeoutException) =>
        process.destroy()
        process.exitValue() // Wait for termination
        ProcessResult(-1, stdout.toString.trim, stderr.toString.trim, timedOut = true)
      case Failure(ex) =>
        throw ex
    }
  }

  def destroy(): Unit = process.destroy()

  def isAlive: Boolean = Try(process.exitValue()).isFailure
}

class ProcessExecutionException(
    message: String,
    val result: ProcessResult
) extends Exception(s"$message\nstderr: ${result.stderr}")
