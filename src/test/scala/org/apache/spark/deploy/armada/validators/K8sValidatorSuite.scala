package org.apache.spark.deploy.armada.validators

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks._

class K8sValidatorSuite extends AnyFunSuite with Matchers {
  test(
    "label and annotation name - isValidKey should accept valid annotation keys and reject invalid ones"
  ) {
    val keyTests = Table(
      ("key", "expected"),
      ("description", true),
      ("example.com/description", true),
      ("valid-prefix/name_1", true),
      ("foo.bar/baz", true),
      ("invalid_prefix/name", false),
      ("invalid key", false),
      ("", false),
      ("/leadingSlash", false),
      ("trailingSlash/", false),
      ("a" * 254 + "/foo", false)
    )

    forAll(keyTests) { (key: String, expected: Boolean) =>
      K8sValidator.Annotation.isValidKey(key) shouldBe expected
    }
  }

  test(
    "annotation - isValidValue should accept values under size limit and reject oversized ones"
  ) {
    val small = "x" * 500
    val big   = "x" * (256 * 1024 + 1)

    val valueTests = Table(
      ("value", "expected"),
      ("", true),
      ("some:value", true),
      (small, true),
      (big, false)
    )

    forAll(valueTests) { (value: String, expected: Boolean) =>
      K8sValidator.Annotation.isValidValue(value) shouldBe expected
    }
  }

  test("label - isValidValue should accept valid values and reject invalid ones") {
    val valueTests = Table(
      ("value", "expected"),
      ("", true),
      ("my-app", true),
      ("App1", true),
      ("-wrong", false),
      ("a" * 64, false),
      ("invalid/value", false)
    )

    forAll(valueTests) { (value: String, expected: Boolean) =>
      K8sValidator.Label.isValidValue(value) shouldBe expected
    }
  }

  test("name - isValid should accept valid DNS-1123 subdomain names and reject invalid ones") {
    // helper strings
    val seg63       = "a" * 63
    val seq64       = "a" * 64
    val tooLongName = "a" * 254

    val nameTests = Table(
      ("name", "expected"),
      // valid subdomain names
      ("my-pod-01", true),
      ("example.com", true),
      ("foo.bar-baz", true),
      (seg63, true),
      (seq64, true),
      // invalid: uppercase
      ("Invalid", false),
      // invalid: empty
      ("", false),
      // invalid: starts or ends with a non-alnum character
      ("-start", false),
      ("end-", false),
      (".dotstart", false),
      ("dotend.", false),
      // invalid: double-dot or slash
      ("foo..bar", false),
      ("/foo", false),
      ("foo/", false),
      // invalid: segment too long (>63) or total too long (>253)
      (tooLongName, false)
    )

    forAll(nameTests) { (name: String, expected: Boolean) =>
      K8sValidator.Name.isValid(name) shouldBe expected
    }
  }

  test("generateName prefix - isValidPrefix should accept valid prefixes and reject invalid ones") {
    val validPrefix = "AZaz09-."
    val dashEnd     = "prefix-"
    val dotEnd      = "prefix."
    val max30       = "a" * 30
    val tooLong30   = "b" * 31

    val prefixTests = Table(
      ("prefix", "expected"),
      (validPrefix, true),
      (dashEnd, true),
      (dotEnd, true),
      (max30, true),
      (tooLong30, false),
      ("", false),
      ("invalid/prefix", false),
      ("with space", false)
    )

    forAll(prefixTests) { (prefix: String, expected: Boolean) =>
      K8sValidator.Name.isValidPrefix(prefix) shouldBe expected
    }
  }
}
