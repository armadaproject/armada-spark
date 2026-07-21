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

"""Stage suite payloads into extraFiles so createImage.sh bakes them in.

Each suite keeps its payloads in <category>/<suite>/jobs/. They are staged into
a per-suite subdirectory so payloads from different suites never collide once
flattened onto the image at /opt/spark/extraFiles/jobs/<suite>/.
"""

from __future__ import annotations

import shutil
from pathlib import Path


def stage_jobs(tests_dir: Path, extra_files_dir: Path) -> list[str]:
    staged: list[str] = []
    if not tests_dir.is_dir():
        return staged

    for jobs_dir in sorted(tests_dir.glob("*/*/jobs")):
        if not jobs_dir.is_dir():
            continue
        payloads = sorted(jobs_dir.glob("*.py"))
        if not payloads:
            continue

        suite = jobs_dir.parent.name
        dest = extra_files_dir / suite
        dest.mkdir(parents=True, exist_ok=True)
        for payload in payloads:
            shutil.copy2(payload, dest / payload.name)
        staged.append(suite)

    return staged
