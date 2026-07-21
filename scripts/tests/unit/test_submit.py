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

import stat
from pathlib import Path

from harness.submit import submit


def _fake_script(tmp_path: Path, body: str) -> Path:
    script = tmp_path / "fake_submit.sh"
    script.write_text("#!/bin/bash\n" + body)
    script.chmod(script.stat().st_mode | stat.S_IXUSR)
    return script


def test_captures_stdout_and_stderr_and_returncode(tmp_path):
    script = _fake_script(tmp_path, 'echo "on stdout"\necho "on stderr" >&2\nexit 3\n')
    result = submit(script, "arg1", mode="cluster", allocation="static")
    assert result.returncode == 3
    assert "on stdout" in result.output
    assert "on stderr" in result.output
    assert result.timed_out is False


def test_forwards_mode_allocation_and_job_args(tmp_path):
    script = _fake_script(tmp_path, 'printf "%s\\n" "$@"\n')
    result = submit(script, "write_read", "table1", mode="client", allocation="dynamic")
    lines = result.output.splitlines()
    assert lines == ["-M", "client", "-A", "dynamic", "write_read", "table1"]


def test_timeout_sets_timed_out_flag(tmp_path):
    script = _fake_script(tmp_path, "sleep 5\n")
    result = submit(script, mode="cluster", allocation="static", timeout=1)
    assert result.timed_out is True
    assert result.returncode == 124


def test_non_string_job_args_are_coerced(tmp_path):
    script = _fake_script(tmp_path, 'printf "%s\\n" "$@"\n')
    result = submit(script, 100000, mode="cluster", allocation="static")
    assert "100000" in result.output.splitlines()
