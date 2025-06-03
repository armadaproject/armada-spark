#!/bin/bash

set -euo pipefail

scripts="$(cd "$(dirname "$0")"; pwd)"
source "$scripts"/init.sh

STATUSFILE="$(mktemp)"
AOREPO='https://github.com/armadaproject/armada-operator.git'
ARMADACTL_VERSION='0.19.1'
JOB_DETAILS=1

if [ "${GITHUB_ACTIONS:-false}" == "true" ]; then
  AOHOME="$scripts/../armada-operator"
  JOBSET='armada-spark'
else
  now=$(date +'%Y%m%d%H%M%S')
  AOHOME="$scripts/../../armada-operator"
  JOBSET="armada-spark-$now" # interactive users may run this multiple times on same cluster
  GITHUB_OUTPUT=/dev/stdout
fi

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

trap 'rm -f -- "$STATUSFILE"' EXIT

usage() {
  cat > /dev/stderr  <<USAGE
Usage: ${0} -s
  -s => get job status details from Armada

USAGE
  exit 1
}

while getopts "s" opt; do
    case "${opt}" in
        s )
          JOB_DETAILS=1 ;;
        * )
          echo "opt is $opt"
          usage ;;
    esac
done

log() {
  echo -e "${GREEN}$1${NC}"
}

err() {
  echo -e "${RED}$1${NC}" >&2
}

log_group() {
  if [ "${GITHUB_ACTIONS:-false}" == "true" ]; then echo -n "::group::"; fi
  echo "$@"
  while read -r line
  do
    echo "$line"
  done
  if [ "${GITHUB_ACTIONS:-false}" == "true" ]; then echo; echo "::endgroup::"; fi
}

fetch-armadactl() {
  os=$(uname -s)
  arch=$(uname -m)
  dl_url='https://github.com/armadaproject/armada/releases/download'

  if [ "$os" = "Darwin" ]; then
    dl_os='darwin'
  elif [ "$os" = "Linux" ]; then
    dl_os='linux'
  else
    err "fetch-armadactl(): sorry, operating system $os not supported; exiting now"
    exit 1
  fi

  if [ "$arch" = 'arm64' ]; then
    dl_arch='arm64'
  elif [ "$arch" = 'x86_64' ]; then
    dl_arch='amd64'
  else
    err "fetch-armadactl(): sorry, architecture $arch not supported; exiting now"
    exit 1
  fi

  dl_file=armadactl_${ARMADACTL_VERSION}_${dl_os}_${dl_arch}.tar.gz

  curl --silent --location "${dl_url}/v${ARMADACTL_VERSION}/$dl_file" | tar -C "$scripts" -xzf - armadactl
}

armadactl-retry() {
  for attempt in {1..10}; do
    if "$scripts"/armadactl "$@" > /dev/null 2>&1; then
      return 0
    fi
    sleep 5
  done
  err "Running \"armadactl $*\" failed after $attempt attempts" >&2
  return 1
}

start-armada() {
  if [ -d "$AOHOME" ]; then
    echo "Using existing armada-operator repo at $AOHOME"
  else
    if ! git clone "$AOREPO" "$AOHOME"; then
      err "There was a problem cloning the armada-operator repo; exiting now"
      exit 1
    fi


    log "Patching armada-operator"
    if ! patch -p1 -d "$AOHOME/" < "$scripts/../e2e/armada-operator.patch"; then
      err "There was an error patching the repo copy $AOHOME"
      exit 1
    fi
  fi

  echo  "Running 'make kind-all' to install and start Armada; this may take up to 6 minutes"
  if ! (cd "$AOHOME"; make kind-all 2>&1) | tee armada-start.txt; then
    echo ""
    err "There was a problem starting Armada; exiting now"
    exit 1
  fi
}

main() {
  if ! (echo "$IMAGE_NAME" | grep -Pq '^\w+:\w+$'); then
    err "IMAGE_NAME is not defined. Please set it in $scripts/config.sh, for example:"
    err "IMAGE_NAME=spark:testing"
    exit 1
  fi

  if [ -z "$ARMADA_QUEUE" ]; then
    err "ARMADA_QUEUE is not defined. Please set it in $scripts/config.sh, for example:"
    err "ARMADA_QUEUE=spark-test"
    exit 1
  fi
  echo "Checking for armadactl .."
  if ! test -x "$scripts"/armadactl; then
    fetch-armadactl | log_group "Fetching armadactl"
  fi

  echo "Checking if image $IMAGE_NAME is available"
  if ! docker image inspect "$IMAGE_NAME" > /dev/null 2>&1; then
    err "Image $IMAGE_NAME not found in local Docker instance."
    err "Rebuild the image (cd '$scripts/..' && mvn clean package && ./scripts/createImage.sh), and re-run this script"
    exit 1
  fi

  echo "Checking to see if Armada cluster is available ..."

  if ! "$scripts"/armadactl get queues > "$STATUSFILE" 2>&1 ; then
    if grep -q 'connection refused' "$STATUSFILE"; then
      start-armada 2>&1 | log_group "Using armada-operator to start Armada; this may take up to 5 minutes"
      sleep 10
      armadactl-retry get queues
    else
      err "FAILED: output is "
      cat "$STATUSFILE"
      echo ""
      err "Armada cluster appears to be running, but an unknown error has happened; exiting now"
      exit 1
    fi
  else
    log "Armada is available"
  fi

  armadactl-retry create queue "$ARMADA_QUEUE" 2>&1 | log_group "Creating $ARMADA_QUEUE queue"

  (test -d "$scripts/.tmp" || mkdir "$scripts/.tmp";
   TMPDIR="$scripts/.tmp" "$AOHOME/bin/tooling/kind" load docker-image "$IMAGE_NAME" --name armada 2>&1) \
   | log_group "Loading Docker image $IMAGE_NAME into Armada cluster";

  sleep 30

  PATH="$scripts:$AOHOME/bin/tooling/:$PATH" JOBSET="$JOBSET" "$scripts/submitSparkPi.sh" 2>&1 | \
    tee submitSparkPi.log
  DRIVER_JOBID=$(grep '^Driver JobID:' submitSparkPi.log | awk '{print $3}')
  EXECUTOR_JOBIDS=$(grep 'executor job with ID:' submitSparkPi.log | awk '{print $6}')

  echo "jobid=$DRIVER_JOBID" >> "$GITHUB_OUTPUT" | log_group "submit"

  if [ "$JOB_DETAILS" = 1 ]; then
    sleep 10   # wait a moment for Armada to schedule & run the job


    echo "---------- about to run armadactl watch ----"
    (timeout 1m "$scripts"/armadactl watch "$ARMADA_QUEUE" "$JOBSET" --exit-if-inactive 2>&1 | tee armadactl.watch.log; \
     if grep "Job failed:" armadactl.watch.log; then err "Job failed"; exit 1; fi) | log_group "Watching Driver Job"

    echo "---------- about to curl for job spec ----"
    curl --silent --show-error -X POST "http://localhost:30000/api/v1/jobSpec" \
      --json "{\"jobId\":\"$DRIVER_JOBID\"}" | jq | log_group "Driver Job Spec  $DRIVER_JOBID"

    for exec_jobid in $EXECUTOR_JOBIDS; do
      curl --silent --show-error -X POST "http://localhost:30000/api/v1/jobSpec" \
          --json "{\"jobId\":\"$exec_jobid\"}" | jq | log_group "Executor Job Spec  $exec_jobid"
    done
  fi

  echo "---------- about to get list of pods ----"
  kubectl get pods -A 2>&1 | log_group "pods"

  echo "---------- about to iterate through pod list ----"
  kubectl get pods -A | tail -n+2 | sed -E -e "s/ +/ /g" | cut -d " " -f 1-2 | while read -r namespace pod
  do
    (kubectl get pod "$pod" --namespace "$namespace" --output json 2>&1 | tee "$namespace.$pod.json"
     kubectl logs "$pod" --namespace "$namespace" 2>&1 | tee "$namespace.$pod.log") | log_group "$pod"
  done
}

main
