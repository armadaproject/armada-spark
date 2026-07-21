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

"""JDBC database integration suite.

Covers the Armada-specific delta that benchmark.sh never exercises: JDBC
driver-jar survival on the classpath, executor egress to an out-of-cluster
database, and credential delivery to scheduled pods.

J1 to J4 are sequential and stateful: J1 creates the table, J2 reads it back,
J3 attacks it with bad credentials, J4 drops it. pytest runs tests in
definition order within a module, and the `table_created` guard makes J2 and J4
skip with a clear reason rather than fail for an unrelated one when J1 did not
create the table.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from harness.outcome import assert_success, matched, tail

JOB = "/opt/spark/extraFiles/jobs/db/db_roundtrip.py"

# JOB is an in-image path, so nothing connects it back to the source payload that
# `run.py prep` stages into the image. Without this guard, renaming or moving the
# source surfaces only much later, as a pod dying with file-not-found.
_SOURCE = Path(__file__).resolve().parent / "jobs" / Path(JOB).name
if not _SOURCE.exists():
    raise RuntimeError(
        f"job payload {_SOURCE} is missing, but JOB points at {JOB}. If you renamed or "
        f"moved it, rename the source to match, then re-run "
        f"`python3 scripts/tests/run.py prep` and rebuild the image with "
        f"./scripts/createImage.sh"
    )

pytestmark = pytest.mark.integration

_state = {"table_created": False}


def _require_table():
    if not _state["table_created"]:
        pytest.skip("J1 did not create the table, so this case cannot be meaningful")


def test_j1_write_and_read_back(submit_job, jdbc, table, rows, cluster_mode):
    result = submit_job("-P", JOB, "write_read", jdbc.url, jdbc.user, jdbc.password,
                        table, rows, jdbc.driver)
    assert not result.timed_out, f"write/read job hung until timeout\n{tail(result)}"
    assert result.returncode == 0, f"write/read job failed (rc={result.returncode})\n{tail(result)}"
    assert_success(result, f"ARMADA_TEST_JDBC_ROWS={rows}", cluster_mode=cluster_mode)
    _state["table_created"] = True


def test_j2_partitioned_read(submit_job, jdbc, table, rows, cluster_mode):
    _require_table()
    result = submit_job("-P", JOB, "partitioned_read", jdbc.url, jdbc.user, jdbc.password,
                        table, rows, jdbc.driver)
    assert not result.timed_out, f"partitioned read hung until timeout\n{tail(result)}"
    assert result.returncode == 0, f"partitioned read failed (rc={result.returncode})\n{tail(result)}"
    assert_success(result, "ARMADA_TEST_JDBC_PARTITIONS_OK=true", cluster_mode=cluster_mode)


def test_j3_bad_credentials_fail_fast(submit_job, jdbc, table, rows, cluster_mode):
    # Deliberately contains none of the words in the auth-error pattern below.
    # submitArmadaSpark.sh echoes its full command line, so a bogus password
    # containing "password" would match the pattern from that echo alone and
    # pass this case without any authentication ever being attempted.
    bad_password = "armada-bogus-credential"
    # A bad-credential submission that does not fail fast is itself the bug, so
    # this case does not wait for the full --submit-timeout default. 180s is
    # long enough for a real auth failure and short enough to catch a hang
    # without waiting 15 minutes to find out.
    result = submit_job("-P", JOB, "write_read", jdbc.url, jdbc.user, bad_password,
                        table, rows, jdbc.driver, timeout=180)

    assert result.returncode != 0, f"write with bad credentials unexpectedly succeeded\n{tail(result)}"
    assert not result.timed_out, (
        f"write with bad credentials hung until timeout instead of failing fast\n{tail(result)}"
    )
    # J1 and J2 already proved connectivity, the driver jar, and TLS with good
    # credentials, so a J3 failure should be auth related. An unmatched
    # signature means an unrelated failure, or a database whose auth-error text
    # needs adding below. It is not counted as a pass.
    #
    # The pattern below is narrower than a plain "auth" or "FATAL" match on
    # purpose. matched() searches the entire submit output, and both of those
    # broad alternatives match strings the harness itself prints on any secured
    # cluster: "auth" is a substring of ARMADA_AUTH_TOKEN and of
    # spark.armada.auth.script.path, and "FATAL" shows up in unrelated Spark
    # and Armada log lines. Either would let this case false-pass on a
    # non-auth failure, which is exactly what this test exists to catch.
    # 28P01 is the PostgreSQL SQLSTATE for invalid_password, and PostgreSQL's
    # actual message is "FATAL: password authentication failed for user",
    # which the second alternative below still matches.
    #
    # This check is client-mode only, and that limitation is real rather than an
    # oversight. In cluster mode the driver runs as a remote Armada pod whose
    # stdout the submitting process never sees, so the only thing observable
    # here is "Container driver failed with exit code 1". The database error
    # text is not present to match against at all. This is the same asymmetry
    # assert_success handles by checking the Armada job status line instead of
    # the payload sentinel.
    #
    # So in cluster mode this case verifies what IS observable: that bad
    # credentials cause a fast, non-zero failure. It cannot verify the failure
    # was specifically an auth failure. Proving that would mean pulling the
    # driver's logs back out of Armada, which the harness does not do today.
    if not cluster_mode:
        assert matched(
            result, "28P01|password authentication failed|authentication failed|permission denied"
        ), (
            f"bad-credential write failed for an unrecognized reason, not a known auth error "
            f"(rc={result.returncode})\n{tail(result)}"
        )


def test_j4_drop_table(submit_job, jdbc, table, rows, cluster_mode):
    _require_table()
    result = submit_job("-P", JOB, "drop", jdbc.url, jdbc.user, jdbc.password,
                        table, rows, jdbc.driver)
    assert not result.timed_out, f"drop job hung until timeout\n{tail(result)}"
    assert result.returncode == 0, f"drop job failed (rc={result.returncode})\n{tail(result)}"
    assert_success(result, "ARMADA_TEST_JDBC_DROPPED=true", cluster_mode=cluster_mode)
