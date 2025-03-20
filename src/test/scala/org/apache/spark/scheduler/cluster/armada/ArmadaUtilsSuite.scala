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


import org.scalatest.funsuite.AnyFunSuite
import ArmadaUtilsExceptions._

class ArmadaUtilSuite
 extends AnyFunSuite {
  test("parseMasterUrl") {
    case class TestCase(
      testUrl: String,
      expectedHost: String,
      expectedPort: Int,
      expectException: Boolean)

    val testCases = List[TestCase](
      TestCase("armada://localhost:50051", "localhost", 50051, false),
      TestCase("armada://malformed:url:ohno", "", 0, true),
      TestCase("armada://badurl", "", 0, true),
      TestCase("armada://localhost:badport", "", 0, true)
    )

    for (tc <- testCases) {
      if (tc.expectException) {
        var caughtException = try {
          ArmadaUtils.parseMasterUrl(tc.testUrl)
          false
        } catch {
          case e: MasterUrlParsingException => true
        } 
        assert(caughtException)
      } else { // no exception expected.
        val (host, port) = ArmadaUtils.parseMasterUrl(tc.testUrl)
        assert(tc.expectedHost == host)
        assert(tc.expectedPort == port)
      }
    }
  }
}
