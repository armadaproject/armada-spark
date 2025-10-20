#!/bin/bash

set -euo pipefail

scripts="$(cd "$(dirname "$0")"; pwd)"
source "$scripts"/init.sh

STATUSFILE="$(mktemp)"
AOREPO='https://github.com/armadaproject/armada-operator.git'
AOHOME="$scripts/../../armada-operator"
ARMADACTL_VERSION='0.19.1'

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

trap 'rm -f -- "$STATUSFILE"' EXIT

log() {
  echo -e "${GREEN}$1${NC}"
}

err() {
  echo -e "${RED}$1${NC}" >&2
}

log_group() {
  if [ "${GITHUB_ACTIONS:-false}" == "true" ]; then echo -n "::group::"; fi
  echo "$@"
  cat -
  if [ "${GITHUB_ACTIONS:-false}" == "true" ]; then echo; echo "::endgroup::"; fi
}

fetch-armadactl() {
  os=$(uname -s)
  arch=$(uname -m)
  dl_url='https://github.com/armadaproject/armada/releases/download'

  if [ "$os" = "Darwin" ]; then
    dl_os='darwin'
    dl_arch='all'  # Darwin releases use 'all' for universal binaries
  elif [ "$os" = "Linux" ]; then
    dl_os='linux'
    if [ "$arch" = 'arm64' ]; then
      dl_arch='arm64'
    elif [ "$arch" = 'x86_64' ]; then
      dl_arch='amd64'
    else
      err "fetch-armadactl(): sorry, architecture $arch not supported; exiting now"
      exit 1
    fi
  else
    err "fetch-armadactl(): sorry, operating system $os not supported; exiting now"
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

init-cluster() {
  if ! (echo "$IMAGE_NAME" | grep -Eq '^[[:alnum:]_]+:[[:alnum:]_]+$'); then
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
    fetch-armadactl 2>&1 | log_group "Fetching armadactl"
  fi


  echo "Checking if image $IMAGE_NAME is available"
  if ! docker image inspect "$IMAGE_NAME" > /dev/null 2>&1; then
    err "Image $IMAGE_NAME not found in local Docker instance."
    err "Rebuild the image (cd '$scripts/..' && mvn test-compile && mvn clean package && ./scripts/createImage.sh -p), and re-run this script"
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

  mkdir -p "$scripts/.tmp"

  TMPDIR="$scripts/.tmp" "$AOHOME/bin/tooling/kind" load docker-image "$IMAGE_NAME" --name armada 2>&1 \
   | log_group "Loading Docker image $IMAGE_NAME into Armada cluster";

  # configure the defaults for the e2e test
  cp $scripts/../e2e/spark-defaults.conf $scripts/../conf/spark-defaults.conf

  log "Waiting 60 seconds for Armada to stabilize ..."
  sleep 60
}

run-test() {
  echo "Running Scala E2E test suite..."

  # Add armadactl to PATH so the e2e framework can access it
  PATH="$scripts:$AOHOME/bin/tooling/:$PATH"
  export PATH

  # Change to armada-spark directory
  cd "$scripts/.."

  # Run the Scala E2E test suite
  mvn scalatest:test -Dsuites="org.apache.spark.deploy.armada.e2e.ArmadaSparkE2E" \
    -Dcontainer.image="$IMAGE_NAME" \
    -Dscala.version="$SCALA_VERSION" \
    -Dscala.binary.version="$SCALA_BIN_VERSION" \
    -Dspark.version="$SPARK_VERSION" \
    -Darmada.queue="$ARMADA_QUEUE" \
    -Darmada.master="armada://localhost:30002" \
    -Darmada.lookout.url="http://localhost:30000" \
    -Darmadactl.path="$scripts/armadactl" 2>&1 | \
    tee e2e-test.log

  TEST_EXIT_CODE=${PIPESTATUS[0]}

  if [ "$TEST_EXIT_CODE" -ne 0 ]; then
    err "E2E tests failed with exit code $TEST_EXIT_CODE"
    exit $TEST_EXIT_CODE
  fi

  log "E2E tests completed successfully"
}

main() {
    init-cluster
    run-test
}

main