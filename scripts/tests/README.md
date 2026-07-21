# Spark-on-Armada script-driven tests

Pytest tests that submit real Spark jobs to Armada via `submitArmadaSpark.sh`
and assert on the outcome. Suites are grouped by category and discovered
automatically.

```
scripts/tests/
  run.py            bootstrap the venv, run suites via pytest
  harness/          shared fixtures + helpers
  integration/db/   a suite: test_<name>.py, README.md, jobs/
  smoke/            more categories as suites arrive
```

## Run

```bash
python3 scripts/tests/run.py                                   # everything
python3 scripts/tests/run.py integration                       # one directory
python3 scripts/tests/run.py -k db --mode cluster --allocation dynamic
```

The first run bootstraps a venv at `scripts/tests/.venv/` and installs pytest.
All arguments are forwarded to pytest, so `-k`, `-x` and `-v` work as usual.
A suite with missing infrastructure SKIPs, never a false pass.

## Run against a local kind cluster

Payloads are baked into the image, so stage the jobs and load a fresh image
before running:

```bash
# 0. one-time: bring up Armada on kind          (see scripts/dev-e2e.sh)
python3 scripts/tests/run.py prep                 # stage jobs into extraFiles/
./scripts/createImage.sh                          # build the image
kind load docker-image spark:armada --name armada # push it into the cluster
python3 scripts/tests/run.py -k db --mode cluster  # run a suite
```

Repeat the three build/load steps only after changing a `jobs/*.py` payload.
Editing a `test_*.py` or anything under `harness/` needs no rebuild.

## Add a suite

1. Create `scripts/tests/<category>/<name>/`.
2. Add `test_<name>.py` using the `submit_job` fixture.
3. Optional `jobs/*.py` payloads, baked into the image at
   `/opt/spark/extraFiles/jobs/<name>/` by `run.py prep`. Add a `jobs/.gitignore`
   for `__pycache__/` and `*.pyc`.
4. Add a `README.md`.

## Cluster vs client mode

A cluster-mode driver runs as a remote pod whose stdout is not visible, so suites
assert on `Driver job ... SUCCEEDED`; in client mode they assert on the local
driver's stdout sentinel. See `assert_success` / `driver_succeeded` in
`harness/outcome.py`.
