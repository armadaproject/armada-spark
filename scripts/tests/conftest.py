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

"""pytest options and fixtures for the script-driven suites."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional

import pytest

from harness import SCRIPTS_DIR, SUBMIT_SCRIPT
from harness import config as harness_config
from harness.postgres import postgres_on_kind_network, validate_db_options
from harness.submit import submit as run_submit


def pytest_addoption(parser):
    group = parser.getgroup("armada-spark")
    group.addoption("--mode", default=None, choices=["cluster", "client"],
                    help="deploy mode forwarded to submitArmadaSpark.sh")
    group.addoption("--allocation", default=None, choices=["static", "dynamic"],
                    help="allocation mode forwarded to submitArmadaSpark.sh")
    group.addoption("--start-db", action="store_true", default=False,
                    help="start a Postgres container on the kind network for the run")
    group.addoption("--submit-timeout", type=int, default=900,
                    help="per-submission timeout in seconds")


@dataclass(frozen=True)
class Jdbc:
    url: str
    user: str
    password: str
    driver: str
    secret_key: Optional[str]


@pytest.fixture(scope="session")
def config():
    # Resolved through the module rather than a bound name so tests can
    # substitute it, and so there is a single call site to reason about.
    return harness_config.load_config(SCRIPTS_DIR)


@pytest.fixture(scope="session")
def mode(pytestconfig, config):
    return pytestconfig.getoption("--mode") or config.get("DEPLOY_MODE", "cluster")


@pytest.fixture(scope="session")
def allocation(pytestconfig, config):
    return pytestconfig.getoption("--allocation") or config.get("ALLOCATION_MODE", "dynamic")


@pytest.fixture(scope="session")
def cluster_mode(mode):
    return mode == "cluster"


@pytest.fixture(scope="session")
def submit_job(pytestconfig, mode, allocation, config):
    default_timeout = pytestconfig.getoption("--submit-timeout")

    def _submit(*job_args, timeout=None):
        return run_submit(SUBMIT_SCRIPT, *job_args, mode=mode, allocation=allocation,
                          timeout=timeout if timeout is not None else default_timeout,
                          env=config)

    return _submit


def pytest_collection_modifyitems(config, items):
    """Reject the mutually exclusive combination, but only for integration runs.

    Deferred from pytest_configure to collection because reading config.sh
    shells out to bash. Validating unconditionally made every pure unit test
    depend on bash and on config.sh existing, for a check that can only matter
    once an integration test is actually going to run.

    Still raised as a UsageError rather than a test failure, because it is a
    mistake in how the run was invoked, not a defect in what is under test.
    """
    if not any(item.get_closest_marker("integration") for item in items):
        return
    try:
        validate_db_options(
            start_db=config.getoption("--start-db"),
            preset_url=harness_config.load_config(SCRIPTS_DIR).get("JDBC_URL", ""),
        )
    except ValueError as exc:
        raise pytest.UsageError(str(exc)) from exc


@pytest.fixture(scope="session")
def jdbc(pytestconfig, config, cluster_mode):
    start_db = pytestconfig.getoption("--start-db")
    preset_url = config.get("JDBC_URL", "")

    if start_db:
        with postgres_on_kind_network() as pg:
            yield Jdbc(url=pg.url, user=pg.user, password=pg.password,
                       driver="org.postgresql.Driver", secret_key=None)
        return

    if not preset_url:
        pytest.skip("no reachable db configured; set JDBC_URL or pass --start-db")

    secret_key = config.get("JDBC_SECRET_KEY") or None
    if secret_key and not cluster_mode:
        pytest.skip(
            "JDBC_SECRET_KEY is set in client mode; secretKeyRef reaches pods only, "
            "not the local driver. Use --mode cluster or the argv JDBC_PASSWORD."
        )

    yield Jdbc(
        url=preset_url,
        user=config.get("JDBC_USER", ""),
        password="" if secret_key else config.get("JDBC_PASSWORD", ""),
        driver=config.get("JDBC_DRIVER", "org.postgresql.Driver"),
        secret_key=secret_key,
    )


@pytest.fixture(scope="session")
def rows(config):
    return int(config.get("JDBC_ROWS", "100000"))


@pytest.fixture(scope="session")
def table():
    return f"armada_spark_test_{os.getpid()}"
