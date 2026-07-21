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

import os

from harness.config import load_config


def test_returns_values_exported_by_config_sh(tmp_path):
    (tmp_path / "config.sh").write_text(
        'export JDBC_URL="jdbc:postgresql://10.0.0.1:5432/db"\n'
        'export JDBC_USER="spark"\n'
    )
    env = load_config(tmp_path)
    assert env["JDBC_URL"] == "jdbc:postgresql://10.0.0.1:5432/db"
    assert env["JDBC_USER"] == "spark"


def test_falls_back_to_os_environ_when_config_missing(tmp_path):
    env = load_config(tmp_path)
    assert env == dict(os.environ)


def test_falls_back_to_os_environ_when_config_fails_to_source(tmp_path):
    # A non-zero `return` from a sourced file makes `source` itself fail. The
    # subshell must propagate that, otherwise a broken config.sh reads as an
    # empty one and every value silently defaults.
    (tmp_path / "config.sh").write_text('export JDBC_USER="spark"\nreturn 1\n')
    assert load_config(tmp_path) == dict(os.environ)


def test_environment_overrides_config_sh(tmp_path, monkeypatch):
    monkeypatch.setenv("SPARK_VERSION", "9.9.9")
    (tmp_path / "config.sh").write_text('export SPARK_VERSION="3.5.5"\n')
    env = load_config(tmp_path)
    assert env["SPARK_VERSION"] == "9.9.9"


def test_config_sh_still_supplies_values_absent_from_environment(tmp_path, monkeypatch):
    monkeypatch.delenv("JDBC_USER", raising=False)
    (tmp_path / "config.sh").write_text('export JDBC_USER="spark"\n')
    assert load_config(tmp_path)["JDBC_USER"] == "spark"


def test_values_containing_newlines_survive(tmp_path):
    (tmp_path / "config.sh").write_text('export MULTI="a\nb"\n')
    env = load_config(tmp_path)
    assert env["MULTI"] == "a\nb"
