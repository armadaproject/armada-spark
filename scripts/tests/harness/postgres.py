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

"""Opt-in Postgres container for the db suite.

The container joins the kind docker network and the JDBC URL uses its container
IP. A database published on localhost is NOT reachable from cluster-mode driver
and executor pods, because pods resolve names through the cluster's CoreDNS and
reach the host only over the kind network.
"""

from __future__ import annotations

import os
import subprocess
import time
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Iterator

DEFAULT_IMAGE = "postgres:16"
DEFAULT_NETWORK = "kind"
POSTGRES_PORT = 5432


def docker_bin() -> str:
    """The container CLI to shell out to, overridable with DOCKER=podman.

    Read per call rather than captured at import so the override applies no
    matter when in the run the environment was set.
    """
    return os.environ.get("DOCKER", "docker")


@dataclass(frozen=True)
class Postgres:
    url: str
    user: str
    password: str
    container: str


def build_run_argv(
    *, container: str, image: str, network: str,
    user: str, password: str, database: str,
) -> list[str]:
    return [
        docker_bin(), "run", "-d",
        "--name", container,
        "--network", network,
        "-e", f"POSTGRES_USER={user}",
        "-e", f"POSTGRES_PASSWORD={password}",
        "-e", f"POSTGRES_DB={database}",
        image,
    ]


def jdbc_url(container_ip: str, database: str) -> str:
    if not container_ip:
        raise ValueError("could not resolve the Postgres container IP on the network")
    return f"jdbc:postgresql://{container_ip}:{POSTGRES_PORT}/{database}"


def validate_db_options(*, start_db: bool, preset_url: str) -> None:
    """Reject the combination that has no sensible interpretation.

    Raises ValueError rather than a pytest error so this module stays free of
    a pytest dependency and is directly unit testable. conftest translates it.
    """
    if start_db and preset_url:
        raise ValueError(
            "--start-db and a preset JDBC_URL are mutually exclusive; "
            "unset JDBC_URL in scripts/config.sh or drop --start-db"
        )


def _container_ip(container: str, network: str) -> str:
    fmt = "{{.NetworkSettings.Networks." + network + ".IPAddress}}"
    proc = subprocess.run(
        [docker_bin(), "inspect", "-f", fmt, container],
        capture_output=True, text=True, check=False,
    )
    return proc.stdout.strip()


def _wait_ready(container: str, user: str, timeout: int) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        proc = subprocess.run(
            [docker_bin(), "exec", container, "pg_isready", "-U", user],
            capture_output=True, text=True, check=False,
        )
        if proc.returncode == 0:
            return
        time.sleep(1)
    raise TimeoutError(f"Postgres container {container} was not ready within {timeout}s")


@contextmanager
def postgres_on_kind_network(
    *, container: str = "armada-spark-test-pg", image: str = DEFAULT_IMAGE,
    network: str = DEFAULT_NETWORK, user: str = "spark",
    password: str = "sparkpw", database: str = "sparktest", ready_timeout: int = 60,
) -> Iterator[Postgres]:
    subprocess.run([docker_bin(), "rm", "-f", container], capture_output=True, check=False)
    # `docker run` must be INSIDE the try. If it fails after the daemon has already
    # registered the container, the finally below is what removes the orphan. Leaving
    # it outside leaks a container under a fixed name, and the next run's leading
    # `docker rm -f` then silently absorbs it, hiding the leak entirely.
    try:
        subprocess.run(
            build_run_argv(container=container, image=image, network=network,
                           user=user, password=password, database=database),
            capture_output=True, text=True, check=True,
        )
        _wait_ready(container, user, ready_timeout)
        yield Postgres(
            url=jdbc_url(_container_ip(container, network), database),
            user=user, password=password, container=container,
        )
    finally:
        subprocess.run([docker_bin(), "rm", "-f", container], capture_output=True, check=False)
