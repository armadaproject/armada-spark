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
package org.apache.spark.deploy.armada.validators

import java.util.regex.Pattern

/** Abstract parent for anything that needs to validate a k8s-style key.
  *
  * @param namePattern
  *   regex that the NAME segment must match
  */
abstract class K8sValidator(protected val namePattern: Pattern) {
  import K8sValidator._

  /** Validates that a key conforms to [<DNS-1123 subdomain>/]NAME syntax.
    *
    *   - Optional prefix: DNS-1123 subdomain (≤ maxPrefixLength chars)
    *   - Required NAME segment: ≤ maxNameLength chars, matching namePattern
    *
    * @param key
    *   the full key to validate
    * @return
    *   true if valid; false otherwise
    */
  def isValidKey(key: String): Boolean = {
    if (key == null) return false

    // split into optional prefix and required name
    val (prefixOpt, name) =
      if (key.contains("/")) {
        val Array(pref, nm) = key.split("/", 2)
        (Some(pref), nm)
      } else (None, key)

    // name non-empty, length ≤ 63, matches the provided NAME regex
    if (name.isEmpty || name.length > maxNameLength) return false
    if (!namePattern.matcher(name).matches()) return false

    // prefix (if present) ≤ 253 chars and must be a DNS-1123 subdomain
    prefixOpt.forall { pref =>
      pref.length <= maxPrefixLength &&
      subdomainPattern.matcher(pref).matches()
    }
  }

  /** Validates the value associated with the key.
    *
    * @param value
    *   the string value to validate
    * @return
    *   true if valid; false otherwise
    */
  def isValidValue(value: String): Boolean
}

object K8sValidator {
  // Maximum lengths per Kubernetes docs
  private val maxNameLength   = 63  // for NAME segments
  private val maxPrefixLength = 253 // for DNS-1123 subdomain prefixes

  // DNS-1123 label (lowercase) and subdomain regex
  private val dns1123Label = "[a-z0-9]([a-z0-9\\-]*[a-z0-9])?"
  // DNS-1123 subdomain: series of labels separated by '.', total ≤253 chars
  private val subdomainFragment = s"$dns1123Label(?:\\.$dns1123Label)*"
  private val subdomainPattern  = Pattern.compile(s"^$subdomainFragment$$")

  // shared NAME regex: 1–63 chars, begin/end alnum [A-Za-z0-9], interior [-_.A-Za-z0-9]
  private val nameFragment = "[A-Za-z0-9]([A-Za-z0-9_.-]*[A-Za-z0-9])?"
  private val namePattern  = Pattern.compile(s"^$nameFragment$$")

  /** Concrete validator for Kubernetes labels.
    *
    * Label values must be empty or ≤63 chars matching the NAME syntax.
    */
  object Label extends K8sValidator(namePattern) {
    override def isValidValue(value: String): Boolean = {
      if (value == null) false
      else if (value.isEmpty) true
      else
        value.length <= maxNameLength &&
        namePattern.matcher(value).matches()
    }
  }

  /** Concrete validator for Kubernetes annotations.
    *
    * Annotation values can be any UTF-8 string up to 256 KiB.
    */
  object Annotation extends K8sValidator(namePattern) {
    private val maxBytes = 256 * 1024

    /** Validates an annotation value.
      *
      * @param value
      *   the annotation's value to validate
      * @return
      *   true if non-null and UTF-8 byte length ≤ maxBytes; false otherwise
      */
    override def isValidValue(value: String): Boolean = {
      value != null && value.getBytes("UTF-8").length <= maxBytes
    }
  }

  /** Validator for metadata.name (a DNS-1123 subdomain).
    *
    * Must consist of lower-case alphanumeric characters, '-' or '.', start and end with an
    * alphanumeric character, and be ≤253 chars.
    */
  object Name {

    /** Validates a fully-specified metadata.name.
      *
      * Uses the same DNS-1123 subdomain rules as prefixes.
      *
      * @param name
      *   the metadata.name to validate
      * @return
      *   true if non-null, non-empty, ≤ maxPrefixLength, and matches subdomainPattern
      */
    def isValid(name: String): Boolean = {
      if (name == null) return false
      // non-empty, max total length 253
      if (name.isEmpty || name.length > maxPrefixLength) return false
      // match the DNS-1123 subdomain pattern
      subdomainPattern.matcher(name).matches()
    }

    /** Validates a generateName prefix.
      *
      *   - non-null, non-empty
      *   - length ≤ 30 characters
      *   - may contain uppercase/lowercase letters, digits, hyphens, underscores, or dots
      *   - may start or end with a hyphen or dot
      *
      * @param prefix
      *   the generateName prefix to validate
      * @return
      *   true if valid; false otherwise
      */
    def isValidPrefix(prefix: String): Boolean = {
      if (prefix == null) return false
      val length = prefix.length
      if (length == 0 || length > 30) return false
      prefixPattern.matcher(prefix).matches()
    }
    // Pattern for generateName prefixes: allows any alphanumeric (any case), '-', or '.', up to 30 chars
    private val prefixPattern = Pattern.compile("^[A-Za-z0-9.-]+$")
  }
}
