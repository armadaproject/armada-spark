#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from __future__ import annotations

import pytest

from harness.outcome import assert_success, driver_failed, driver_succeeded, matched, tail
from harness.submit import SubmitResult


def _result(output: str, returncode: int = 0) -> SubmitResult:
    return SubmitResult(argv=["fake"], returncode=returncode, output=output, timed_out=False)


def test_matched_is_case_insensitive():
    assert matched(_result("ARMADA_TEST_JDBC_ROWS=100"), "armada_test_jdbc_rows=100")


def test_matched_handles_alternation():
    assert matched(_result("FATAL: password authentication failed"), "auth|28P01|FATAL")


def test_driver_succeeded_requires_zero_exit():
    assert driver_succeeded(_result("Driver job abc SUCCEEDED"))
    assert not driver_succeeded(_result("Driver job abc SUCCEEDED", returncode=1))


def test_driver_succeeded_requires_the_status_line():
    assert not driver_succeeded(_result("submitted fine, no status"))


def test_driver_failed_requires_the_status_line():
    assert driver_failed(_result("Driver job abc FAILED: boom", returncode=1))
    # The case this exists to catch: a submission that never got as far as
    # running the driver, so nothing about the job under test was exercised.
    assert not driver_failed(_result("error: queue rejected submission", returncode=1))


def test_driver_failed_requires_nonzero_exit():
    assert not driver_failed(_result("Driver job abc FAILED: boom"))


def test_assert_success_cluster_mode_checks_driver_status():
    assert_success(_result("Driver job abc SUCCEEDED"), "IGNORED", cluster_mode=True)
    with pytest.raises(AssertionError, match="Driver job"):
        assert_success(_result("no status here"), "IGNORED", cluster_mode=True)


def test_assert_success_client_mode_checks_sentinel():
    assert_success(_result("ARMADA_TEST_JDBC_DROPPED=true"), "ARMADA_TEST_JDBC_DROPPED=true",
                   cluster_mode=False)
    with pytest.raises(AssertionError, match="sentinel"):
        assert_success(_result("nothing"), "ARMADA_TEST_JDBC_DROPPED=true", cluster_mode=False)


def test_tail_limits_output():
    assert tail(_result("\n".join(str(i) for i in range(100))), lines=3) == "97\n98\n99"
