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
cp postgresql-<version>.jar extraJars/     # or your DB's JDBC driver
./scripts/tests/prep_jobs.sh
./scripts/createImage.sh
```

Configure `ARMADA_MASTER`, `ARMADA_QUEUE`, `ARMADA_NAMESPACE`, and (secured
cluster) `ARMADA_AUTH_TOKEN` as for `submitArmadaSpark.sh`.

## Run

```bash
JDBC_URL=jdbc:postgresql://host:5432/db JDBC_USER=u JDBC_PASSWORD=p \
  ./scripts/tests/integration/db/db_test.sh -M cluster -A static
```

SKIPs when `JDBC_URL` is unset. Timeout guarding (the `HUNG until timeout`
detection) needs GNU `timeout` or `gtimeout`; on macOS install it with
`brew install coreutils`, otherwise submissions run unguarded.

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
| `DB_VERIFY_USER` / `DB_VERIFY_PASSWORD` / `DB_VERIFY_DRIVER_JAR` | _(unset)_ | optional out-of-Spark row-count check; skips if unset. Jar auto-discovery only finds `postgresql-*.jar`; for other DBs set `DB_VERIFY_DRIVER_JAR` |
| `SUBMIT_TIMEOUT` | `900` | per-submission timeout (seconds) |
