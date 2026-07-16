# Spark-on-Armada script-driven tests

Shell tests that submit real Spark jobs to Armada via `submitArmadaSpark.sh` and
assert on the outcome. Suites are grouped by category and discovered
automatically.

```
scripts/tests/
  run.sh                run suites, aggregate pass/fail
  prep_jobs.sh          stage suite jobs/ into the image
  lib/common.sh         shared assertions + helpers
  integration/db/       a suite: <name>_test.sh, README.md, jobs/
  smoke/                more categories as suites arrive
```

## Run

```bash
./scripts/tests/run.sh -M cluster -A static          # all suites
./scripts/tests/run.sh integration -M cluster        # one category
./scripts/tests/run.sh db -M cluster -A dynamic      # one suite
./scripts/tests/integration/db/db_test.sh -M cluster # a suite directly
```

The optional first argument selects a suite name, a category, or a
`category/suite` path; omit it to run everything. Remaining flags are forwarded
to `submitArmadaSpark.sh`. `run.sh` exits non-zero if a suite fails or the
selector matches nothing; a suite with missing infra SKIPs (never a false pass).

## Run against a local kind cluster

Suites submit to a running Armada. Payloads are baked into the image, so on a
local kind cluster (named `armada`) stage the jobs and load a fresh image before
running:

```bash
# 0. one-time: bring up Armada on kind          (see scripts/dev-e2e.sh)
./scripts/tests/prep_jobs.sh                     # stage each suite's jobs/ into extraFiles/
./scripts/createImage.sh                         # build the image with the staged jobs
kind load docker-image spark:armada --name armada # push it into the cluster
./scripts/tests/run.sh db -M cluster -A dynamic  # run a suite
```

Repeat the three build/load steps only after changing a `jobs/*.py` payload (or
the image); editing a `*_test.sh` or `lib/common.sh` needs no rebuild.
Suite-specific prerequisites (driver jars in `extraJars/`, a reachable DB) live
in each suite's README.

## Add a suite

1. Create `scripts/tests/<category>/<name>/`.
2. Add executable `<name>_test.sh` sourcing the harness:
   `source "$(dirname "$0")/../../lib/common.sh"`.
3. Optional `jobs/*.py` payloads (baked into the image at
   `/opt/spark/extraFiles/jobs/<name>/` by `prep_jobs.sh`; add a `jobs/.gitignore`
   for `__pycache__/` and `*.pyc`).
4. Add a `README.md`.

After changing any `jobs/` file, rebuild and reload the image (see "Run against a
local kind cluster" above).

## Cluster vs client mode

A cluster-mode driver runs as a remote pod whose stdout is not visible, so suites
assert on `Driver job ... SUCCEEDED`; in client mode they assert on the local
driver's stdout sentinel. See `is_cluster_mode` / `driver_succeeded` in
`lib/common.sh`.
