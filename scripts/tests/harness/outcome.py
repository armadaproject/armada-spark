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

"""Assertions over a SubmitResult.

A cluster-mode driver runs as a remote pod whose stdout is not visible to the
submitting process, so cluster-mode cases assert on the Armada job status line
instead of the payload's sentinel.
"""

from __future__ import annotations

import re

from .submit import SubmitResult

DRIVER_SUCCEEDED_PATTERN = r"Driver job .* SUCCEEDED"
DRIVER_FAILED_PATTERN = r"Driver job \S+ FAILED"


def matched(result: SubmitResult, pattern: str) -> bool:
    return re.search(pattern, result.output, re.IGNORECASE | re.MULTILINE) is not None


def driver_succeeded(result: SubmitResult) -> bool:
    return result.returncode == 0 and matched(result, DRIVER_SUCCEEDED_PATTERN)


def driver_failed(result: SubmitResult) -> bool:
    """True when the driver pod ran and reported failure.

    Distinct from a bare non-zero exit: queue rejection, an image pull failure,
    or any submission-level error exits non-zero without the driver ever
    running, and none of those print this status line.
    """
    return result.returncode != 0 and matched(result, DRIVER_FAILED_PATTERN)


def tail(result: SubmitResult, lines: int = 30) -> str:
    return "\n".join(result.output.splitlines()[-lines:])


def assert_success(result: SubmitResult, sentinel: str, *, cluster_mode: bool) -> None:
    if cluster_mode:
        if not driver_succeeded(result):
            raise AssertionError(
                f"submission exited {result.returncode} but no "
                f"'Driver job ... SUCCEEDED' status was seen\n"
                f"--- last 30 lines ---\n{tail(result)}"
            )
        return

    if not matched(result, sentinel):
        raise AssertionError(
            f"submission succeeded but sentinel ({sentinel}) was missing\n"
            f"--- last 30 lines ---\n{tail(result)}"
        )
