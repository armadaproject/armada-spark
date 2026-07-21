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

from harness.jobs import stage_jobs


def _suite(tests_dir, category: str, suite: str, filename: str = "payload.py"):
    jobs = tests_dir / category / suite / "jobs"
    jobs.mkdir(parents=True)
    (jobs / filename).write_text("# payload\n")
    return jobs


def test_stages_payloads_into_per_suite_directories(tmp_path):
    tests_dir = tmp_path / "tests"
    dest = tmp_path / "extraFiles" / "jobs"
    _suite(tests_dir, "integration", "db", "db_roundtrip.py")

    staged = stage_jobs(tests_dir, dest)

    assert staged == ["db"]
    assert (dest / "db" / "db_roundtrip.py").read_text() == "# payload\n"


def test_suites_do_not_collide(tmp_path):
    tests_dir = tmp_path / "tests"
    dest = tmp_path / "extraFiles" / "jobs"
    _suite(tests_dir, "integration", "db", "payload.py")
    _suite(tests_dir, "smoke", "basic", "payload.py")

    staged = stage_jobs(tests_dir, dest)

    assert sorted(staged) == ["basic", "db"]
    assert (dest / "db" / "payload.py").exists()
    assert (dest / "basic" / "payload.py").exists()


def test_ignores_suites_with_no_python_payloads(tmp_path):
    tests_dir = tmp_path / "tests"
    dest = tmp_path / "extraFiles" / "jobs"
    jobs = tests_dir / "integration" / "empty" / "jobs"
    jobs.mkdir(parents=True)
    (jobs / ".gitignore").write_text("__pycache__/\n")

    assert stage_jobs(tests_dir, dest) == []


def test_returns_empty_when_no_suites_exist(tmp_path):
    assert stage_jobs(tmp_path / "tests", tmp_path / "dest") == []
