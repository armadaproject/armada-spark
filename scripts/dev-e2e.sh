#!/bin/bash

set -euo pipefail

scripts="$(cd "$(dirname "$0")"; pwd)"
source "$scripts"/init.sh

AOHOME="$scripts/../../armada-operator"
STATUSFILE="$(mktemp)"
AOREPO='https://github.com/armadaproject/armada-operator.git'
ARMADACTL_VERSION='0.19.1'
JOB_DETAILS=1

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
    log "Fetching armadactl..."
    fetch-armadactl
  fi

  echo "Checking if image $IMAGE_NAME is available ..."
  if ! docker image inspect "$IMAGE_NAME" > /dev/null 2>&1; then
    err "Image $IMAGE_NAME not found in local Docker instance."
    err "Rebuild the image (cd '$scripts/..' && mvn clean package && ./scripts/createImage.sh), and re-run this script"
    exit 1
  fi

  echo "Checking to see if Armada cluster is available ..."

  if ! "$scripts"/armadactl get queues > "$STATUSFILE" 2>&1 ; then
    if grep -q 'connection refused' "$STATUSFILE"; then
      echo "Using armada-operator to start Armada cluster; this may take up to 5 minutes"
      start-armada
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

  echo "Creating $ARMADA_QUEUE queue..."
  armadactl-retry create queue "$ARMADA_QUEUE"

  echo "Loading Docker image $IMAGE_NAME into Armada cluster"
  TMPDIR="$scripts/.tmp" "$AOHOME/bin/tooling/kind" load docker-image "$IMAGE_NAME" --name armada

  echo "Submitting SparkPI job"
  PATH="$AOHOME/bin/tooling/:$PATH" "$scripts/submitSparkPi.sh"  2>&1 | tee submitSparkPi.log

  DRIVER_JOBID=$(grep '^Driver JobID:' submitSparkPi.log | awk '{print $3}')
  EXECUTOR_JOBIDS=$(grep '^Executor JobID:' submitSparkPi.log | awk '{print $3}')

  if [ "$JOB_DETAILS" = 1 ]; then
    sleep 5   # wait a moment for Armada to schedule & run the job

    timeout 1m "$scripts"/armadactl watch test driver --exit-if-inactive 2>&1 | tee armadactl.watch.log
    if grep "Job failed:" armadactl.watch.log; then err "Job failed"; exit 1; fi

    echo "Driver Job Spec  $DRIVER_JOBID"
    curl --silent --show-error -X POST "http://localhost:30000/api/v1/jobSpec" \
      --json "{\"jobId\":\"$DRIVER_JOBID\"}" | jq
    for exec_jobid in $EXECUTOR_JOBIDS; do
      log "Executor Job Spec  $exec_jobid"
      curl --silent --show-error -X POST "http://localhost:30000/api/v1/jobSpec" \
          --json "{\"jobId\":\"$exec_jobid\"}" | jq
    done
  fi
}

main
