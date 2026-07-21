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

import dataclasses

import pytest

from harness.postgres import Postgres, build_run_argv, docker_bin, jdbc_url, validate_db_options


def _run_argv() -> list[str]:
    return build_run_argv(
        container="spark-pg-test", image="postgres:16", network="kind",
        user="spark", password="sparkpw", database="sparktest",
    )


def test_run_argv_joins_the_kind_network():
    argv = build_run_argv(
        container="spark-pg-test", image="postgres:16", network="kind",
        user="spark", password="sparkpw", database="sparktest",
    )
    assert "--network" in argv
    assert argv[argv.index("--network") + 1] == "kind"


def test_run_argv_passes_credentials_as_environment():
    argv = build_run_argv(
        container="spark-pg-test", image="postgres:16", network="kind",
        user="spark", password="sparkpw", database="sparktest",
    )
    joined = " ".join(argv)
    assert "POSTGRES_USER=spark" in joined
    assert "POSTGRES_PASSWORD=sparkpw" in joined
    assert "POSTGRES_DB=sparktest" in joined
    assert argv[-1] == "postgres:16"


def test_run_argv_defaults_to_docker(monkeypatch):
    monkeypatch.delenv("DOCKER", raising=False)
    assert _run_argv()[0] == "docker"


def test_docker_env_var_overrides_the_container_cli(monkeypatch):
    monkeypatch.setenv("DOCKER", "podman")
    assert docker_bin() == "podman"
    assert _run_argv()[0] == "podman"


def test_jdbc_url_uses_the_container_ip_not_localhost():
    assert jdbc_url("172.18.0.5", "sparktest") == "jdbc:postgresql://172.18.0.5:5432/sparktest"


def test_jdbc_url_rejects_empty_ip():
    with pytest.raises(ValueError, match="container IP"):
        jdbc_url("", "sparktest")


def test_validate_db_options_rejects_start_db_with_preset_url():
    with pytest.raises(ValueError, match="mutually exclusive"):
        validate_db_options(start_db=True, preset_url="jdbc:postgresql://preset:5432/db")


def test_validate_db_options_allows_each_alone():
    validate_db_options(start_db=True, preset_url="")
    validate_db_options(start_db=False, preset_url="jdbc:postgresql://preset:5432/db")
    validate_db_options(start_db=False, preset_url="")


def test_postgres_dataclass_is_frozen():
    pg = Postgres(url="jdbc:postgresql://1.2.3.4:5432/db", user="u",
                  password="p", container="c")
    with pytest.raises(dataclasses.FrozenInstanceError):
        pg.url = "changed"
