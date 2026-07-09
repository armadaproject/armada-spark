# Preemption smoke test (issue #148)

Proves the Spark driver survives when its **scale-up executors** are preempted
by Armada on a local kind cluster.

## Prerequisites

- A kind + Armada cluster. `run.sh` brings it up automatically via
  `scripts/create-cluster.sh` (idempotent) if it isn't already running.
- Built `spark:armada` image: `mvn clean package && ./scripts/createImage.sh -p`
- `scripts/config.sh` set for local kind (`USE_KIND=true`, `ARMADA_MASTER=armada://...:30002`, `ARMADA_QUEUE=test`)
- Tools: `docker`, `kind`, `kubectl`, `jq`, and the checked-in `scripts/armadactl`

The Armada scheduler's priority classes (`armada-default` 1000, `armada-preemptible`
900) are provisioned by `e2e/armada-operator.patch` at cluster-creation time. `run.sh`
verifies this and, if the scheduler wasn't patched, stops with instructions to recreate
the cluster.

## Usage

    ./scripts/test/preemption/run.sh                   # run the test (exit 0 = driver survived)
    ./scripts/test/preemption/run.sh --target-cores 4  # cap worker2 to ~4 allocatable cores
    ./scripts/test/preemption/run.sh --keep            # skip cleanup (leaves cap + jobs for debugging)

## What it does

1. Preflight and ensure the cluster via `scripts/create-cluster.sh` (idempotent).
2. Caps `armada-worker2` allocatable CPU to `--target-cores` (kubelet `systemReserved`, restored on exit).
3. Verifies the scheduler knows `armada-preemptible`.
4. Submits a victim dynamic-cluster Spark job (driver + initial gang = `armada-default`, scale-up = `armada-preemptible`).
5. Waits for a scale-up executor pod (A1).
6. Submits an `armada-default` aggressor sized to force urgency preemption of the scale-up exec.
7. Asserts: scale-up preempted (A2), driver stays Running (A3), driver never logs
   `Driver job ... was PREEMPTED` (A4), app not aborted (A5). Driver survival = A3 ∧ A4 ∧ A5.

## Cleanup

Runs on every exit (success, failure, Ctrl-C): restores the worker node's CPU cap and
cancels the victim + aggressor job sets. `--keep` disables it.
