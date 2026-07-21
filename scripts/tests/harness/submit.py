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

"""Invoke scripts/submitArmadaSpark.sh and capture the outcome."""

from __future__ import annotations

import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

TIMEOUT_RETURNCODE = 124


@dataclass(frozen=True)
class SubmitResult:
    argv: list[str]
    returncode: int
    output: str
    timed_out: bool


def _decode(stream) -> str:
    if stream is None:
        return ""
    if isinstance(stream, bytes):
        return stream.decode("utf-8", errors="replace")
    return stream


def submit(
    submit_script: Path,
    *job_args: object,
    mode: str,
    allocation: str,
    timeout: int = 900,
    env: Optional[dict] = None,
) -> SubmitResult:
    """Run the submit script once and return its outcome.

    Output is captured in memory rather than a temp file, so there is nothing
    to clean up and no leak when a case fails partway through.
    """
    argv = [str(submit_script), "-M", mode, "-A", allocation]
    argv += [str(arg) for arg in job_args]

    try:
        proc = subprocess.run(
            argv,
            capture_output=True,
            text=True,
            timeout=timeout,
            env=env,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        return SubmitResult(
            argv=argv,
            returncode=TIMEOUT_RETURNCODE,
            output=_decode(exc.stdout) + _decode(exc.stderr),
            timed_out=True,
        )

    return SubmitResult(
        argv=argv,
        returncode=proc.returncode,
        output=proc.stdout + proc.stderr,
        timed_out=False,
    )
