package org.apache.spark.deploy.armada.submit

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks

private class ArmadaUtilsSuite extends AnyFunSuite with TableDrivenPropertyChecks with Matchers {
  test("buildServiceNameFromJobId should create a valid service name") {
    val input    = "01jwefnvywg1sa05m593jztcce"
    val expected = "armada-01jwefnvywg1sa05m593jztcce-0-service-0"
    val actual   = ArmadaUtils.buildServiceNameFromJobId(input)
    assert(actual == expected)
  }
}
