# Database (JDBC) integration suite

Submits real Spark JDBC jobs to Armada. Covers the Armada-specific delta that
`benchmark.sh` never exercises: JDBC driver-jar survival on the classpath,
executor egress to an out-of-cluster DB, and credential delivery to scheduled
pods. Cases:

- **J1** write + read-back
- **J2** partitioned read (concurrent executor connections)
- **J3** bad credentials fail fast
- **J4** drop the table

See `scripts/tests/README.md` for the shared harness and run commands.

## Setup

The JDBC driver jar is not in the base image, and `--packages` will not resolve
under `MVN_OFFLINE`. Bake the driver in, then stage the jobs and rebuild:

```bash
curl -Lo extraJars/postgresql-42.7.4.jar \
  https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar
python3 scripts/tests/run.py prep
./scripts/createImage.sh
kind load docker-image spark:armada --name armada
```

Configure `ARMADA_MASTER`, `ARMADA_QUEUE`, `ARMADA_NAMESPACE`, and (secured
cluster) `ARMADA_AUTH_TOKEN` as for `submitArmadaSpark.sh`.

## Run

Either let the harness start a database:

```bash
python3 scripts/tests/run.py -k db --start-db --mode cluster
```

Or point it at your own. The database must be reachable from driver and
executor pods, which means it has to be on the kind docker network. A database
published on `localhost` will NOT work:

```bash
docker run -d --name spark-pg --network kind \
  -e POSTGRES_USER=spark -e POSTGRES_PASSWORD=sparkpw -e POSTGRES_DB=sparktest \
  postgres:16
IP=$(docker inspect -f '{{.NetworkSettings.Networks.kind.IPAddress}}' spark-pg)
```

Values you export in your shell take precedence over `scripts/config.sh`, so you
can point a single run at a different database without editing the file.

To force the suite to SKIP without editing `config.sh`, pass an empty value:
`JDBC_URL= python3 scripts/tests/run.py integration`. Use an empty string rather
than `env -u JDBC_URL`. The environment is overlaid onto `config.sh` as a merge,
and a merge cannot express absence, so unsetting a variable leaves `config.sh`'s
value in place.

```bash
export JDBC_URL="jdbc:postgresql://$IP:5432/sparktest"
export JDBC_USER=spark
export JDBC_PASSWORD=sparkpw
```

```bash
python3 scripts/tests/run.py -k db --mode cluster
```

SKIPs when `JDBC_URL` is unset and `--start-db` is not passed.
`--start-db` and a preset `JDBC_URL` are mutually exclusive.

## Credential delivery

- **argv** (`JDBC_PASSWORD`): puts the secret on the command line (test-only).
- **secretKeyRef** (`JDBC_SECRET_KEY`): injects the password into pods as
  `$ARMADA_JDBC_PASSWORD` from a K8s Secret, mirroring the S3 `armada-secret`
  pattern (`scripts/init.sh`). Cluster-mode only (a client-mode driver is local,
  not a pod).

## Env knobs

| Var | Default | Effect |
|-----|---------|--------|
| `JDBC_URL` | _(unset)_ | `jdbc:...`; unset means SKIP |
| `JDBC_USER` / `JDBC_PASSWORD` / `JDBC_DRIVER` | empty / empty / `org.postgresql.Driver` | credentials (argv) + driver class |
| `JDBC_ROWS` | `100000` | rows to write/read |
| `JDBC_SECRET_KEY` / `JDBC_PASSWORD_ITEM` | _(unset)_ / `password` | K8s Secret + key; injects the password via secretKeyRef instead of argv |

These are set in `scripts/config.sh`. Per-run options are pytest flags instead:

| Flag | Default | Effect |
|------|---------|--------|
| `--mode` | from `config.sh` (`DEPLOY_MODE`), falling back to `cluster` | deploy mode forwarded to `submitArmadaSpark.sh` |
| `--allocation` | from `config.sh` (`ALLOCATION_MODE`), falling back to `dynamic` | allocation mode forwarded to `submitArmadaSpark.sh` |
| `--start-db` | off | start a throwaway Postgres on the kind network for this run |
| `--submit-timeout` | `900` | per-submission timeout (seconds) |

The bash suite also had an optional out-of-Spark row-count check driven by
`DB_VERIFY_USER` / `DB_VERIFY_PASSWORD` / `DB_VERIFY_DRIVER_JAR`, which compiled
a small Java program to query the table directly. It was not carried over. J1
verifies its write by reading it back through Spark.
