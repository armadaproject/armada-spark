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

"""Entry point for the script-driven test suites.

`run.py prep` stages suite payloads and exits. Any other invocation
bootstraps a local venv and forwards all arguments to pytest.
"""

from __future__ import annotations

import os
import subprocess
import sys
import venv
from pathlib import Path

TESTS_DIR = Path(__file__).resolve().parent
VENV_DIR = TESTS_DIR / ".venv"
sys.path.insert(0, str(TESTS_DIR))


def venv_python(venv_dir: Path) -> Path:
    bindir = "Scripts" if os.name == "nt" else "bin"
    return venv_dir / bindir / "python"


def _pytest_importable(python: Path) -> bool:
    if not python.exists():
        return False
    result = subprocess.run(
        [str(python), "-c", "import pytest"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    return result.returncode == 0


def ensure_venv() -> Path:
    python = venv_python(VENV_DIR)
    if _pytest_importable(python):
        return python
    # Either the venv does not exist yet, or a previous bootstrap failed or was
    # interrupted after creating the directory but before pytest was installed.
    # In that second case the python binary exists but pytest does not, so
    # every later run would keep returning this same broken interpreter with
    # no hint that the fix is to remove the venv. Rebuilding here instead of
    # failing makes the bootstrap self-healing.
    print(f"bootstrapping venv at {VENV_DIR}")
    venv.EnvBuilder(with_pip=True, clear=True).create(VENV_DIR)
    subprocess.run(
        [str(python), "-m", "pip", "install", "-q", "-e", str(TESTS_DIR)],
        check=True,
    )
    return python


def main(argv: list[str]) -> int:
    if argv and argv[0] == "prep":
        from harness import ROOT_DIR
        from harness.jobs import stage_jobs

        staged = stage_jobs(TESTS_DIR, ROOT_DIR / "extraFiles" / "jobs")
        if not staged:
            print(f"no suite jobs found under {TESTS_DIR}/*/*/jobs/")
            return 0
        for suite in staged:
            print(f"staged {suite} jobs -> extraFiles/jobs/{suite}/")
        print()
        print("now rebuild the image so they land at /opt/spark/extraFiles/jobs/:")
        print("  ./scripts/createImage.sh")
        return 0

    python = ensure_venv()
    return subprocess.run(
        [str(python), "-m", "pytest", *argv], cwd=str(TESTS_DIR)
    ).returncode


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
