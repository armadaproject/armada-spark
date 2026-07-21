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

pytest_plugins = ["pytester"]

import pathlib

import pytest

import harness.config

CONFTEST = (pathlib.Path(__file__).resolve().parent.parent / "conftest.py").read_text()


@pytest.fixture
def with_conftest(pytester):
    pytester.makeconftest(CONFTEST)
    return pytester


class _Recorder:
    """Stands in for load_config, counting calls and serving a canned env."""

    def __init__(self):
        self.calls = []
        self.env = {}

    def __call__(self, scripts_dir):
        self.calls.append(scripts_dir)
        return self.env


@pytest.fixture
def load_config_spy(monkeypatch):
    """Replace load_config so tests can assert whether bash was ever invoked.

    The conftest under test resolves load_config through its module rather than
    a bound name, so patching the attribute reaches the copy pytester loads.
    """
    spy = _Recorder()
    monkeypatch.setattr(harness.config, "load_config", spy)
    return spy


def test_mode_and_allocation_default_to_cluster_and_dynamic(with_conftest):
    with_conftest.makepyfile(
        test_defaults="""
        def test_defaults(mode, allocation, cluster_mode):
            assert mode == "cluster"
            assert allocation == "dynamic"
            assert cluster_mode is True
        """
    )
    with_conftest.runpytest("-p", "no:cacheprovider").assert_outcomes(passed=1)


def test_mode_option_flips_cluster_mode(with_conftest):
    with_conftest.makepyfile(
        test_client="""
        def test_client(mode, cluster_mode):
            assert mode == "client"
            assert cluster_mode is False
        """
    )
    with_conftest.runpytest("--mode", "client", "-p",
                            "no:cacheprovider").assert_outcomes(passed=1)


def test_submit_timeout_option_defaults_to_900(with_conftest):
    with_conftest.makepyfile(
        test_timeout="""
        def test_timeout(pytestconfig):
            assert pytestconfig.getoption("--submit-timeout") == 900
        """
    )
    with_conftest.runpytest("-p", "no:cacheprovider").assert_outcomes(passed=1)


def test_unit_only_run_never_reads_config_sh(with_conftest, load_config_spy):
    """A run that collected no integration test must not shell out to bash."""
    with_conftest.makepyfile(
        test_plain="""
        def test_plain():
            assert True
        """
    )
    with_conftest.runpytest("--start-db", "-p", "no:cacheprovider").assert_outcomes(passed=1)
    assert load_config_spy.calls == []


def test_start_db_with_preset_url_is_a_usage_error_on_integration_runs(
    with_conftest, load_config_spy
):
    load_config_spy.env = {"JDBC_URL": "jdbc:postgresql://preset:5432/db"}
    with_conftest.makeini(
        """
        [pytest]
        markers =
            integration: submits real Spark jobs
        """
    )
    with_conftest.makepyfile(
        test_integration="""
        import pytest

        pytestmark = pytest.mark.integration

        def test_needs_a_db():
            assert True
        """
    )
    result = with_conftest.runpytest("--start-db", "-p", "no:cacheprovider")
    # USAGE_ERROR specifically, not merely non-zero: this must stay a
    # command-line error rather than degrade into a test failure.
    assert result.ret == pytest.ExitCode.USAGE_ERROR
    result.stderr.fnmatch_lines(["*mutually exclusive*"])
    assert load_config_spy.calls != []
