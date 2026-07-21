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

"""Read scripts/config.sh without reimplementing bash.

config.sh is a bash file, so the only correct way to read it is to let bash
evaluate it and report the resulting environment. `env -0` is used because
values may legitimately contain newlines.
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path


def load_config(scripts_dir: Path) -> dict[str, str]:
    """Source scripts_dir/config.sh in a subshell, return the resulting env.

    Falls back to the current environment when config.sh is absent or fails
    to source, tolerating a missing config as the bash harness once did.

    Precedence: the caller's environment wins. config.sh supplies defaults
    for anything the caller did not set. Note that because there is no way
    to distinguish a variable the caller deliberately exported from one it
    merely inherited, the full parent environment overlays config.sh, not
    only variables set on the command line. That is the intended, standard
    Unix precedence tradeoff, not an oversight.
    """
    config = scripts_dir / "config.sh"
    if not config.exists():
        return dict(os.environ)

    # `|| exit 1` is load-bearing. Without it the compound command's exit status
    # is that of `env -0`, which is 0 even when the source failed outright, so the
    # returncode check below could never fire and a broken config.sh would be
    # silently read as an empty one.
    proc = subprocess.run(
        ["bash", "-c", f'source "{config}" >/dev/null 2>&1 || exit 1; env -0'],
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        return dict(os.environ)

    env: dict[str, str] = {}
    for entry in proc.stdout.split("\0"):
        if not entry:
            continue
        key, sep, value = entry.partition("=")
        if sep:
            env[key] = value

    # The caller's environment wins. config.sh supplies defaults for anything the
    # caller did not set. Historically config.sh used unconditional `export` and
    # silently beat the command line, so `JDBC_URL=... run.py` had no effect.
    return {**env, **os.environ}
