#!/usr/bin/env bash
#
# Preemption smoke test (issue #148): proves the Spark driver survives when its
# scale-up executors are preempted on a local kind+Armada cluster.
# See scripts/test/preemption/README.md
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPTS_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"     # repo/scripts
ARMADACTL="$SCRIPTS_DIR/armadactl"
KIND_CLUSTER="armada"
WORKER_NODE="armada-worker2"
QUEUE="test"
SCHED_NS="armada"
SCHED_CR="armada-scheduler"
# The aggressor runs in the same queue ($QUEUE) as the victim, so preemption is
# intra-queue urgency preemption: the queue reclaims its own lower-priority
# (armada-preemptible, 900) scale-up executors for its higher-priority
# (armada-default, 1000) aggressor.

log()  { echo "[preempt] $*" >&2; }
die()  { echo "[preempt][ERROR] $*" >&2; exit 1; }

OPT_KEEP=false
TARGET_CORES=6

parse_args() {
  OPT_KEEP=false; TARGET_CORES=6
  while [ $# -gt 0 ]; do
    case "$1" in
      --keep)         OPT_KEEP=true; shift ;;
      --target-cores) TARGET_CORES="$2"; shift 2 ;;
      -h|--help)      echo "usage: run.sh [--keep] [--target-cores N]"; exit 0 ;;
      *)              die "unknown arg: $1" ;;
    esac
  done
}

CLEANUP_FNS=()
register_cleanup() { CLEANUP_FNS+=("$1"); }
run_cleanup() {
  if [ "${OPT_KEEP:-false}" = "true" ]; then
    log "--keep set: skipping cleanup (node cap and jobs left in place)"
    return 0
  fi
  local i
  for (( i=${#CLEANUP_FNS[@]}-1 ; i>=0 ; i-- )); do
    "${CLEANUP_FNS[$i]}" || log "cleanup ${CLEANUP_FNS[$i]} failed (ignored)"
  done
}

need() { command -v "$1" >/dev/null 2>&1 || die "required tool not found: $1"; }

preflight() {
  need kubectl; need docker; need jq
  [ -x "$ARMADACTL" ] || die "armadactl not executable at $ARMADACTL"
  # Ensure the cluster exists via the shared helper (idempotent: fast no-op if
  # already up, otherwise brings it up — may take several minutes).
  log "ensuring kind+Armada cluster via create-cluster.sh"
  "$SCRIPTS_DIR/create-cluster.sh" || die "create-cluster.sh failed to bring up the cluster"
  kubectl cluster-info --context "kind-$KIND_CLUSTER" >/dev/null 2>&1 \
    || die "cluster still not reachable after create-cluster.sh"
  kubectl get node "$WORKER_NODE" >/dev/null 2>&1 \
    || die "node $WORKER_NODE not found"
  [ "$(kubectl get node "$WORKER_NODE" -o jsonpath='{.metadata.labels.armada-spark}')" = "true" ] \
    || die "node $WORKER_NODE missing label armada-spark=true"
  "$ARMADACTL" get queue "$QUEUE" >/dev/null 2>&1 || "$ARMADACTL" create queue "$QUEUE" \
    || die "could not ensure queue $QUEUE"
  log "preflight ok"
}

# Normalize a k8s cpu quantity ("6", "3920m") to whole cores (floor). Empty -> 0.
normalize_cores() {
  local v="$1"
  if [ "${v: -1}" = "m" ]; then echo $(( ${v%m} / 1000 )); else echo "${v:-0}"; fi
}

# Pure: cores to reserve so allocatable ≈ target. Never negative.
compute_reserve() {
  local capacity="$1" target="$2" reserve
  reserve=$(( capacity - target ))
  (( reserve < 0 )) && reserve=0
  echo "$reserve"
}

# Kubelet config path inside the kind node container.
KUBELET_CFG="/var/lib/kubelet/config.yaml"
KUBELET_BAK="/var/lib/kubelet/config.yaml.preempt-bak"

restore_worker() {
  log "restoring $WORKER_NODE kubelet config"
  if docker exec "$WORKER_NODE" test -f "$KUBELET_BAK" 2>/dev/null; then
    docker exec "$WORKER_NODE" cp "$KUBELET_BAK" "$KUBELET_CFG" || log "restore: cp failed (continuing)"
    docker exec "$WORKER_NODE" rm -f "$KUBELET_BAK" || log "restore: rm failed (continuing)"
    docker exec "$WORKER_NODE" systemctl restart kubelet || log "restore: kubelet restart failed"
    log "kubelet restart attempted with original config"
  fi
}

cap_worker() {
  local cap reserve
  cap="$(kubectl get node "$WORKER_NODE" -o jsonpath='{.status.capacity.cpu}')"
  [ -n "$cap" ] || die "could not read $WORKER_NODE cpu capacity"
  reserve="$(compute_reserve "$cap" "$TARGET_CORES")"
  log "worker capacity=${cap} cores; reserving ${reserve} → target allocatable ~${TARGET_CORES}"
  (( reserve == 0 )) && { log "no reservation needed (capacity ≤ target); skipping cap"; return 0; }

  # Reduce allocatable by appending a systemReserved block to the KubeletConfiguration.
  # Appended via shell rather than a python/YAML edit: the kind node image ships python3
  # without PyYAML, so an `import yaml` edit would silently no-op and leave capacity intact.
  docker exec "$WORKER_NODE" sh -c "test -f $KUBELET_BAK || cp $KUBELET_CFG $KUBELET_BAK"
  register_cleanup restore_worker
  # Reset to the clean backup first so a re-run can't stack duplicate systemReserved
  # keys, then append. The leading newline guards against a missing trailing newline.
  docker exec "$WORKER_NODE" sh -c "cp $KUBELET_BAK $KUBELET_CFG"
  docker exec "$WORKER_NODE" sh -c "printf '\nsystemReserved:\n  cpu: \"%s\"\n' '$reserve' >> $KUBELET_CFG"
  docker exec "$WORKER_NODE" systemctl restart kubelet

  # Wait for the node to re-register reduced allocatable.
  local i alloc alloc_raw
  for i in $(seq 1 60); do
    alloc_raw="$(kubectl get node "$WORKER_NODE" -o jsonpath='{.status.allocatable.cpu}' 2>/dev/null || true)"
    [ -n "$alloc_raw" ] || { sleep 2; continue; }
    alloc="$(normalize_cores "$alloc_raw")"
    if [ "$alloc" -le "$(( TARGET_CORES + 1 ))" ] 2>/dev/null; then
      log "worker allocatable now ${alloc} cores"; return 0
    fi
    sleep 2
  done
  die "worker allocatable did not drop to ~${TARGET_CORES} (last=${alloc:-unknown})"
}

ensure_priority_classes() {
  # Armada does not set k8s spec.priorityClassName on the pods it creates (priority
  # lives in Armada's own scheduler config; pods are tagged with an armada_preemptible
  # label instead), so no Kubernetes PriorityClass objects are needed here.
  #
  # The Armada scheduler's priority classes are provisioned by e2e/armada-operator.patch
  # at cluster-creation time (armada-preemptible priority 900, priorityClassNameOverride
  # cleared). Verify the running scheduler was created with that patch — without it the
  # scheduler's default forces every job onto armada-default (priority 1000, override set),
  # so no scale-up executor is ever preemptible and the test cannot work.
  local prio
  prio="$(kubectl -n "$SCHED_NS" get scheduler "$SCHED_CR" \
    -o jsonpath='{.spec.applicationConfig.scheduling.priorityClasses.armada-preemptible.priority}' \
    2>/dev/null || true)"
  [ "$prio" = "900" ] || die "scheduler is not provisioned for preemption \
(armada-preemptible priority='${prio:-unset}', expected 900).
  This is provisioned by e2e/armada-operator.patch at cluster creation. Recreate the cluster so
  the patch applies: 'kind delete cluster --name armada && rm -rf ../armada-operator', then re-run
  (run.sh calls scripts/create-cluster.sh, which re-clones the operator and applies the patch)."
  log "priority classes ready (scheduler armada-preemptible@900 confirmed)"
}

VICTIM_LOG=""
VICTIM_JOBSET=""

cancel_victim() {
  [ -n "$VICTIM_JOBSET" ] || return 0
  log "cancelling victim jobset $VICTIM_JOBSET"
  "$ARMADACTL" cancel job-set "$QUEUE" "$VICTIM_JOBSET" >/dev/null 2>&1 || true
}

submit_victim() {
  VICTIM_JOBSET="${RUN_ID}-victim"
  VICTIM_LOG="$(mktemp -t preempt-victim-XXXX.log)"

  # Known jobSetId → deterministic watch target. The scale-up class (armada-preemptible)
  # is what makes the scale-up executors preemptible; the initial gang stays armada-default.
  # Pin BOTH request and limit cpu to 1 core for driver and executors. Armada rejects a
  # container whose cpu limit is smaller than its request, and whose memory limit is larger
  # than its request — so request and limit must match. We set only cpu (equal request/limit
  # = a known 1 core each, for the capped-node math) and leave memory at init.sh's default
  # (1Gi request == 1Gi limit).
  local extra="--conf
spark.armada.jobSetId=${VICTIM_JOBSET}
--conf
spark.armada.scheduling.initialPriorityClass=armada-default
--conf
spark.armada.scheduling.scaleUpPriorityClass=armada-preemptible
--conf
spark.armada.executor.request.cores=1
--conf
spark.armada.executor.limit.cores=1
--conf
spark.armada.driver.request.cores=1
--conf
spark.armada.driver.limit.cores=1"

  log "submitting victim job (jobSet=$VICTIM_JOBSET)"
  register_cleanup cancel_victim
  # Large SparkPi partition count keeps the single stage (and its executors) busy well
  # past the aggressor/observe window; if the executors finished early and idle-released,
  # there would be nothing left to preempt.
  SPARK_SUBMIT_EXTRA_CONF="$extra" \
    "$SCRIPTS_DIR/submitArmadaSpark.sh" -M cluster -A dynamic 400000 \
    >"$VICTIM_LOG" 2>&1 &
  VICTIM_SUBMIT_PID=$!
  log "victim submit running (pid $VICTIM_SUBMIT_PID, log $VICTIM_LOG)"
}

A1_PASS=false

# Identify scale-up executors by Armada's `armada_preemptible=true` pod label.
# NOTE: Armada does NOT set the k8s `spec.priorityClassName` field on the pods it
# creates — priority classes live in Armada's own scheduler config. It instead
# stamps `armada_preemptible=<bool>` (from the job's priority class) as a label:
# scale-up execs (armada-preemptible@900) get "true" and carry no gangId; the
# initial gang (armada-default) gets "false" and an armadaproject.io/gangId
# annotation. So "true" uniquely selects non-gang, preemptible scale-up execs.
await_scaleup() {
  log "waiting for scale-up executor pods (armada_preemptible=true) on $WORKER_NODE"
  local i pod
  for i in $(seq 1 90); do
    pod="$(kubectl get pods -l armada_preemptible=true \
      --field-selector "spec.nodeName=$WORKER_NODE,status.phase=Running" \
      -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
    if [ -n "$pod" ]; then
      A1_PASS=true
      log "A1 PASS: scale-up pod $pod is preemptible (armada_preemptible=true), not gang-linked"
      return 0
    fi
    sleep 4
  done
  die "timed out waiting for preemptible scale-up executor pods (see $VICTIM_LOG)"
}

# Size the aggressor to the largest whole-core cpu that fits ONLY after the worker's
# preemptible executors are reclaimed: floor(allocatable - nonPreempt), inputs in
# millicores. With at least one 1-core preemptible exec present (A1), this exceeds the
# worker's real free space (so it forces preemption) yet still fits once the execs are
# evicted. Sizing off the non-preemptible footprint rather than measured free avoids a
# rounding trap where fractional system-pod usage yields an aggressor that never fits.
aggressor_cpu_from() {
  local alloc_m="$1" nonpmt_m="$2" room_m cpu
  room_m=$(( alloc_m - nonpmt_m ))
  (( room_m < 0 )) && room_m=0
  cpu=$(( room_m / 1000 ))   # floor to whole cores
  (( cpu < 1 )) && cpu=1
  echo "$cpu"
}

# Worker allocatable cpu, in millicores.
worker_alloc_millicores() {
  local v; v="$(kubectl get node "$WORKER_NODE" -o jsonpath='{.status.allocatable.cpu}')"
  if [ "${v: -1}" = "m" ]; then echo "${v%m}"; else echo $(( ${v:-0} * 1000 )); fi
}

# Sum of cpu requests (millicores) of the worker's NON-preemptible running/pending pods:
# every pod except those labelled armada_preemptible=true (the reclaimable scale-up execs).
# Summed precisely (no per-pod flooring) so fractional system-pod usage is not lost.
worker_nonpreemptible_millicores() {
  kubectl get pods --all-namespaces -o json \
    | jq -r --arg n "$WORKER_NODE" '[.items[]
        | select(.spec.nodeName==$n)
        | select(.status.phase=="Running" or .status.phase=="Pending")
        | select((.metadata.labels.armada_preemptible // "") != "true")
        | .spec.containers[].resources.requests.cpu // "0"]
      | map(if endswith("m") then (rtrimstr("m")|tonumber) else (tonumber*1000) end)
      | add // 0 | floor'
}

AGGRESSOR_JOBSET=""
cancel_aggressor() {
  [ -n "$AGGRESSOR_JOBSET" ] || return 0
  log "cancelling aggressor jobset $AGGRESSOR_JOBSET"
  "$ARMADACTL" cancel job-set "$QUEUE" "$AGGRESSOR_JOBSET" >/dev/null 2>&1 || true
}

run_aggressor() {
  local alloc_m nonpmt_m cpu spec
  alloc_m="$(worker_alloc_millicores)"
  nonpmt_m="$(worker_nonpreemptible_millicores)"
  cpu="$(aggressor_cpu_from "$alloc_m" "$nonpmt_m")"
  AGGRESSOR_JOBSET="${RUN_ID}-aggressor"
  spec="$(mktemp -t preempt-aggressor-XXXX.yaml)"
  sed -e "s/__AGGRESSOR_CPU__/$cpu/g" -e "s/__JOBSET__/$AGGRESSOR_JOBSET/g" \
    -e "s/__QUEUE__/$QUEUE/g" \
    "$SCRIPT_DIR/aggressor.yaml.tmpl" > "$spec"
  log "submitting aggressor (queue=$QUEUE, jobSet=$AGGRESSOR_JOBSET, cpu=$cpu, alloc=${alloc_m}m, nonPreempt=${nonpmt_m}m)"
  register_cleanup cancel_aggressor
  "$ARMADACTL" submit "$spec" >/dev/null || die "aggressor submit failed"
}

# A2: watch the VICTIM jobset for a JobPreempted event, bounded.
observe_preemption() {
  # `timeout(1)` is not available everywhere (absent on stock macOS), so bound the
  # stream ourselves: run `armadactl watch` in the background into a temp file and
  # poll it for a preemption event, then stop it. Returns 0 if seen within secs.
  local secs="${1:-90}" tmp wpid waited=0 rc=1
  log "watching victim jobset $VICTIM_JOBSET for JobPreempted (<=${secs}s)"
  tmp="$(mktemp -t preempt-watch-XXXX)"
  "$ARMADACTL" watch "$QUEUE" "$VICTIM_JOBSET" >"$tmp" 2>/dev/null &
  wpid=$!
  while [ "$waited" -lt "$secs" ]; do
    if grep -iqE 'preempted' "$tmp" 2>/dev/null; then rc=0; break; fi
    sleep 3
    waited=$(( waited + 3 ))
  done
  kill "$wpid" 2>/dev/null || true
  wait "$wpid" 2>/dev/null || true
  if [ "$rc" -ne 0 ] && grep -iqE 'preempted' "$tmp" 2>/dev/null; then rc=0; fi
  rm -f "$tmp"
  return "$rc"
}

driver_pod() {
  kubectl get pods -o json | jq -r --arg js "$VICTIM_JOBSET" '.items[]
    | select(.metadata.annotations.armada_jobset_id==$js)
    | select(.metadata.labels."spark-role"=="driver") | .metadata.name' | head -1
}

assert_and_report() {
  local a2="FAIL" a3="FAIL" a4="FAIL" a5="FAIL" drv drvphase drvlog

  if observe_preemption 120; then a2="PASS"; fi

  drv="$(driver_pod || true)"
  if [ -n "$drv" ]; then
    drvphase="$(kubectl get pod "$drv" -o jsonpath='{.status.phase}' 2>/dev/null || true)"
    if [ "$drvphase" = "Running" ] || [ "$drvphase" = "Succeeded" ]; then a3="PASS"; fi
    drvlog="$(kubectl logs "$drv" 2>/dev/null || true)"
    # A5: a Succeeded driver is conclusive without needing logs.
    if [ "$drvphase" = "Succeeded" ]; then a5="PASS"; fi
    if [ -n "$drvlog" ]; then
      # A4 needs log evidence: PASS only when logs exist AND lack the marker.
      echo "$drvlog" | grep -q 'Driver job .* was PREEMPTED' || a4="PASS"
      # A5 for a still-Running driver needs logs to confirm it isn't aborting.
      if [ "$a5" != "PASS" ] && [ "$drvphase" = "Running" ] \
         && ! echo "$drvlog" | grep -qi 'aborting\|application failed'; then a5="PASS"; fi
    else
      log "A4/A5 inconclusive: no driver logs for $drv — failing closed"
    fi
  fi

  echo
  echo "==================== PREEMPTION SMOKE TEST RESULT ===================="
  printf "  A1 scale-up class + no gang : %s\n" "$([ "$A1_PASS" = true ] && echo PASS || echo FAIL)"
  printf "  A2 scale-up preempted       : %s\n" "$a2"
  printf "  A3 driver still Running/Succ : %s\n" "$a3"
  printf "  A4 no 'Driver ... PREEMPTED' : %s\n" "$a4"
  printf "  A5 app not aborted          : %s\n" "$a5"
  echo "  Driver survival = A3 && A4 && A5"
  echo "====================================================================="

  if [ "$a3" = "PASS" ] && [ "$a4" = "PASS" ] && [ "$a5" = "PASS" ] \
     && [ "$A1_PASS" = true ] && [ "$a2" = "PASS" ]; then
    log "RESULT: PASS — driver survived scale-up preemption"; return 0
  fi
  log "RESULT: FAIL"; return 1
}

main() {
  parse_args "$@"
  # shellcheck disable=SC2034
  RUN_ID="preempt-$(date +%s)"
  trap run_cleanup EXIT
  preflight
  cap_worker
  ensure_priority_classes
  submit_victim
  await_scaleup
  run_aggressor
  assert_and_report
}

main "$@"
